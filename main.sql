-- =============================================================================
-- main.sql — Dashboard de Gobernanza ML_MAIN_SLIDER
-- =============================================================================
-- Propósito: visibilidad de TODAS las campañas activas sirviendo en
--            ML_MAIN_SLIDER en el momento de ejecución, agrupadas por
--            cluster de prioridad, con métricas del período solicitado.
--
-- Parámetros de ejecución:
--   @site_id              STRING   ej: 'MLA'
--   @ros_filter           STRING   'all' | 'ros_only' | 'segmented_only'
--   @time_window          STRING   'today' | 'last_7_days'
--   @max_rows_per_cluster INT64    default 20
--
-- ⚠️  MODO SCRIPT (BigQuery Console):
--     Descomentá el bloque DECLARE y reemplazá @param por el nombre de variable.
-- =============================================================================

/*
DECLARE site_id              STRING  DEFAULT 'MLA';
DECLARE ros_filter           STRING  DEFAULT 'all';
DECLARE time_window          STRING  DEFAULT 'today';
DECLARE max_rows_per_cluster INT64   DEFAULT 20;
*/

WITH

-- ---------------------------------------------------------------------------
-- 1. Mapa de timezones por site_id
-- ---------------------------------------------------------------------------
tz_map AS (
  SELECT site_id, tz
  FROM UNNEST([
    STRUCT('MLA' AS site_id, 'America/Argentina/Buenos_Aires' AS tz),
    STRUCT('MLB' AS site_id, 'America/Sao_Paulo'              AS tz),
    STRUCT('MLM' AS site_id, 'America/Mexico_City'            AS tz),
    STRUCT('MLC' AS site_id, 'America/Santiago'               AS tz),
    STRUCT('MLU' AS site_id, 'America/Montevideo'             AS tz),
    STRUCT('MCO' AS site_id, 'America/Bogota'                 AS tz),
    STRUCT('MPE' AS site_id, 'America/Lima'                   AS tz)
  ])
),

-- ---------------------------------------------------------------------------
-- 2. Timezone resuelto para el site solicitado
-- ---------------------------------------------------------------------------
site_config AS (
  SELECT tz
  FROM tz_map
  WHERE site_id = @site_id
),

-- ---------------------------------------------------------------------------
-- 3. Line items activos en ML_MAIN_SLIDER "ahora"
--
--    Filtros de vigencia:
--      a) campaign.status = 'active'
--      b) campaign_type IN ('PROGRAMMATIC', 'GUARANTEED')
--         – PROGRAMMATIC: campañas de subastas y reservas estándar
--         – GUARANTEED:   reservas garantizadas (candidato a PRIORIDAD_0)
--      c) campaign.tags IS NULL
--         – tags es un array JSON de flags de sistema (PAUSED_EXPIRED,
--           DEPRECATED, DESTINATION_CLOSED, etc.). IS NULL indica que la
--           campaña no tiene ningún flag desfavorable activo.
--      d) campaign.site_id = @site_id
--      e) line_item.pages contiene ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)
--      f) CURRENT_TIMESTAMP() entre start_date y end_date del line item
--
--    Joins mínimos: campaign (status, site_id, type, priority como contexto)
--                   line_item (pages, vigencia, targeting para ROS)
--
--    Plan y budget excluidos: ver README → "Decisiones > Plan/Budget".
-- ---------------------------------------------------------------------------
active_line_items AS (
  SELECT
    li.line_item_id,
    li.campaign_id,
    c.campaign_type,
    c.priority                    AS campaign_priority,
    c.advertiser_id               AS owner,
    c.account_id,

    -- Nivel de slider extraído del JSON de pages (STRING serializado)
    REGEXP_EXTRACT(
      TO_JSON_STRING(li.pages),
      r'ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)'
    )                             AS slider_level,

    li.end_date,

    -- -----------------------------------------------------------------------
    -- ROS vs Segmentado
    -- Definición auditada:
    --   is_ros = TRUE  si NO hay ningún indicador de targeting activo
    --   is_ros = FALSE si audience_dmp tiene valor (ID numérico de segmento DMP)
    --                  o si audience/context/geolocation no están vacíos
    --
    -- Evidencia: en 30 muestras de line_items MAIN_SLIDER activos,
    -- segmentation_strategy fue siempre NULL. El único campo discriminante
    -- observado fue audience_dmp (NULL = sin segmento, número = con segmento).
    -- -----------------------------------------------------------------------
    CASE
      WHEN li.audience_dmp IS NOT NULL
        THEN FALSE
      WHEN TO_JSON_STRING(li.audience)    NOT IN ('null', '[]', '')
        THEN FALSE
      WHEN TO_JSON_STRING(li.context)     NOT IN ('null', '[]', '')
        THEN FALSE
      WHEN TO_JSON_STRING(li.geolocation) NOT IN ('null', '[]', '')
        THEN FALSE
      ELSE TRUE
    END AS is_ros

  FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item` li
  INNER JOIN `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign` c
    ON li.campaign_id = c.campaign_id

  WHERE
    c.status         = 'active'
    AND c.campaign_type IN ('PROGRAMMATIC', 'GUARANTEED')
    AND TO_JSON_STRING(c.tags) = 'null'
    AND c.site_id    = @site_id
    AND REGEXP_CONTAINS(
          TO_JSON_STRING(li.pages),
          r'ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)'
        )
    AND CURRENT_TIMESTAMP() BETWEEN TIMESTAMP(li.start_date)
                                AND TIMESTAMP(li.end_date)
),

-- ---------------------------------------------------------------------------
-- 4. Clasificación en clusters de prioridad
--
--    Regla de mapeo:
--      GUARANTEED                   → PRIORIDAD_0
--        (campo campaign_type = 'GUARANTEED'; goal.strategy = 'guaranteed'
--         confirmado en datos. Ver validate.sql para evidencia. Si no aparece
--         data real en esta sección, queda vacía en el dashboard.)
--
--      PROGRAMMATIC + PRIMARY       → PRIORIDAD_PRINCIPAL
--      PROGRAMMATIC + HIGH          → PRIORIDAD_ALTA
--      PROGRAMMATIC + MEDIUM        → PRIORIDAD_MEDIA
--
--    FALLBACK: no existe campo real (deal_type siempre NULL, ningún campo
--    con valor fallback/default/preloaded encontrado). Sección vacía.
-- ---------------------------------------------------------------------------
clustered AS (
  SELECT
    ali.*,
    CASE
      WHEN ali.campaign_type = 'GUARANTEED'    THEN 'PRIORIDAD_0'
      WHEN ali.slider_level  = 'PRIMARY'       THEN 'PRIORIDAD_PRINCIPAL'
      WHEN ali.slider_level  = 'HIGH'          THEN 'PRIORIDAD_ALTA'
      WHEN ali.slider_level  = 'MEDIUM'        THEN 'PRIORIDAD_MEDIA'
      ELSE                                          'UNKNOWN'
    END AS priority_cluster
  FROM active_line_items ali
  WHERE ali.slider_level IS NOT NULL
),

-- ---------------------------------------------------------------------------
-- 5. Métricas agregadas por line_item_id
--    – Filtro de partición: EVENT_LOCAL_DT según ventana y timezone del site
--    – Predicado IN (subquery): escanea solo filas de line_items activos
-- ---------------------------------------------------------------------------
metrics AS (
  SELECT
    m.LINE_ITEM_ID  AS line_item_id,
    SUM(m.PRINTS_QTY) AS impressions,
    SUM(m.CLICKS_QTY) AS clicks
  FROM `meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY` m
  CROSS JOIN site_config sc
  WHERE
    m.SIT_SITE_ID = @site_id
    AND (
        (@time_window = 'today'
          AND m.EVENT_LOCAL_DT = CURRENT_DATE(sc.tz))
      OR
        (@time_window = 'last_7_days'
          AND m.EVENT_LOCAL_DT BETWEEN
                DATE_SUB(CURRENT_DATE(sc.tz), INTERVAL 6 DAY)
                AND CURRENT_DATE(sc.tz))
    )
    AND m.LINE_ITEM_ID IN (SELECT line_item_id FROM clustered)
  GROUP BY 1
),

-- ---------------------------------------------------------------------------
-- 6. Join + filtro ROS
-- ---------------------------------------------------------------------------
joined AS (
  SELECT
    c.priority_cluster,
    c.line_item_id,
    c.campaign_id,
    c.campaign_type,
    c.campaign_priority,
    COALESCE(m.impressions, 0)                              AS impressions,
    COALESCE(m.clicks, 0)                                   AS clicks,
    SAFE_DIVIDE(
      COALESCE(m.clicks, 0),
      NULLIF(COALESCE(m.impressions, 0), 0)
    )                                                       AS ctr,
    c.owner,
    CAST(c.end_date AS STRING)                              AS end_date,
    c.is_ros,
    @time_window                                            AS time_window_label,
    @ros_filter                                             AS ros_filter_applied
  FROM clustered c
  LEFT JOIN metrics m ON c.line_item_id = m.line_item_id
  WHERE
       @ros_filter = 'all'
    OR (@ros_filter = 'ros_only'       AND c.is_ros = TRUE)
    OR (@ros_filter = 'segmented_only' AND c.is_ros = FALSE)
),

-- ---------------------------------------------------------------------------
-- 7. Share of impressions dentro del cluster + ranking por cluster
-- ---------------------------------------------------------------------------
with_share AS (
  SELECT
    *,
    SAFE_DIVIDE(
      impressions,
      SUM(impressions) OVER (PARTITION BY priority_cluster)
    )                                                       AS share_of_impressions,
    ROW_NUMBER() OVER (
      PARTITION BY priority_cluster
      ORDER BY impressions DESC
    )                                                       AS rn
  FROM joined
)

-- ---------------------------------------------------------------------------
-- 8. Output final
-- ---------------------------------------------------------------------------
SELECT
  priority_cluster,
  line_item_id,
  campaign_id,
  campaign_type,
  campaign_priority,
  impressions,
  clicks,
  ROUND(ctr,                6) AS ctr,
  owner,
  end_date,
  is_ros,
  ROUND(share_of_impressions, 6) AS share_of_impressions,
  time_window_label,
  ros_filter_applied

FROM with_share
WHERE rn <= @max_rows_per_cluster

ORDER BY
  CASE priority_cluster
    WHEN 'PRIORIDAD_0'          THEN 1
    WHEN 'PRIORIDAD_PRINCIPAL'  THEN 2
    WHEN 'PRIORIDAD_ALTA'       THEN 3
    WHEN 'PRIORIDAD_MEDIA'      THEN 4
    ELSE                             5
  END,
  share_of_impressions DESC
;
