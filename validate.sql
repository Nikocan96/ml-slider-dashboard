-- =============================================================================
-- validate.sql — Queries de validación para ml-slider-dashboard
-- =============================================================================
-- Ejecutar de a una sección. Todas usan filtros chicos (DISTINCT, LIMIT,
-- rangos de fecha acotados) para mantener costo bajo.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- V1. Verificar el regex de pages para ML_MAIN_SLIDER
--     Confirma qué valores exactos existen y que el patrón captura bien.
-- ---------------------------------------------------------------------------
SELECT
  JSON_VALUE(page_json, '$.page') AS page_value,
  COUNT(*)                        AS line_items_count
FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item`,
     UNNEST(JSON_QUERY_ARRAY(pages)) AS page_json
WHERE TO_JSON_STRING(pages) LIKE '%MAIN_SLIDER%'
GROUP BY 1
ORDER BY 2 DESC;

-- Resultado esperado:
--   ML_MAIN_SLIDER_PRIMARY   → mapea a PRIORIDAD_PRINCIPAL
--   ML_MAIN_SLIDER_HIGH      → mapea a PRIORIDAD_ALTA
--   ML_MAIN_SLIDER_MEDIUM    → mapea a PRIORIDAD_MEDIA
--   (nada más → confirma que el regex cubre todo)


-- ---------------------------------------------------------------------------
-- V2. Conteo de line_items activos "ahora" por slider_level
--     (cambia @site_id por el valor deseado)
-- ---------------------------------------------------------------------------
SELECT
  REGEXP_EXTRACT(
    TO_JSON_STRING(li.pages),
    r'ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)'
  )                              AS slider_level,
  c.campaign_type,
  COUNT(DISTINCT li.line_item_id) AS li_count,
  COUNT(DISTINCT c.campaign_id)   AS campaign_count
FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item` li
INNER JOIN `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign` c
  ON li.campaign_id = c.campaign_id
WHERE
  c.status       = 'active'
  AND c.campaign_type IN ('PROGRAMMATIC', 'GUARANTEED')
  AND TO_JSON_STRING(c.tags) = 'null'
  AND c.site_id  = 'MLA'    -- ← cambiar por el site deseado
  AND REGEXP_CONTAINS(TO_JSON_STRING(li.pages), r'ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)')
  AND CURRENT_TIMESTAMP() BETWEEN TIMESTAMP(li.start_date) AND TIMESTAMP(li.end_date)
GROUP BY 1, 2
ORDER BY 3 DESC;


-- ---------------------------------------------------------------------------
-- V3. ROS vs Segmentado — conteos y evidencia de campos usados
-- ---------------------------------------------------------------------------
SELECT
  CASE
    WHEN li.audience_dmp IS NOT NULL
      THEN 'segmented_dmp'
    WHEN TO_JSON_STRING(li.audience) NOT IN ('null', '[]', '')
      THEN 'segmented_audience'
    WHEN TO_JSON_STRING(li.context)  NOT IN ('null', '[]', '')
      THEN 'segmented_context'
    WHEN TO_JSON_STRING(li.geolocation) NOT IN ('null', '[]', '')
      THEN 'segmented_geo'
    ELSE 'ros'
  END                             AS targeting_type,
  COUNT(*)                        AS li_count

FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item` li
WHERE TO_JSON_STRING(li.pages) LIKE '%MAIN_SLIDER%'
GROUP BY 1
ORDER BY 2 DESC;

-- Campos candidatos inspeccionados:
--   audience_dmp   → ID numérico de segmento DMP (el más común)
--   audience       → JSON — puede contener reglas de audiencia
--   context        → JSON — targeting contextual
--   geolocation    → JSON — targeting geográfico
--   segmentation_strategy → siempre NULL en muestras de MAIN_SLIDER (inservible)


-- ---------------------------------------------------------------------------
-- V4. Exploración PRIORIDAD_0
--     Busca indicadores reales de reserva/garantía en campañas con MAIN_SLIDER
-- ---------------------------------------------------------------------------

-- V4a. ¿Hay campañas GUARANTEED con line_items en ML_MAIN_SLIDER?
SELECT
  c.campaign_type,
  JSON_VALUE(c.goal, '$.strategy')  AS goal_strategy,
  c.priority,
  COUNT(DISTINCT li.line_item_id)   AS li_count
FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item` li
INNER JOIN `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign` c
  ON li.campaign_id = c.campaign_id
WHERE
  TO_JSON_STRING(li.pages) LIKE '%MAIN_SLIDER%'
  AND c.status = 'active'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

-- Interpretación:
--   Si aparecen filas con campaign_type = 'GUARANTEED' → confirma PRIORIDAD_0 real
--   Si solo aparece PROGRAMMATIC → PRIORIDAD_0 queda vacío en el dashboard

-- V4b. ¿Hay campos numéricos de priority en campaign con valores especiales?
SELECT DISTINCT priority
FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign`
WHERE campaign_type IN ('PROGRAMMATIC', 'GUARANTEED')
ORDER BY 1;

-- Resultado conocido: 'BIDDING', 'MARKETING', 'MPLAY' (y otros)
-- 'GUARANTEED' como campaign_type es el único indicador real de P0.


-- ---------------------------------------------------------------------------
-- V5. Exploración FALLBACK
--     Busca indicadores de campaña default/preloaded/fallback
-- ---------------------------------------------------------------------------

-- V5a. Valores de DEAL_TYPE en métricas para line_items de MAIN_SLIDER
--      (últimos 7 días, site MLA — costo acotado)
SELECT
  m.DEAL_TYPE,
  m.LINE_ITEM_TYPE,
  m.AD_PRIORITY,
  COUNT(*)           AS rows,
  SUM(m.PRINTS_QTY)  AS prints
FROM `meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY` m
WHERE
  m.SIT_SITE_ID = 'MLA'
  AND m.EVENT_LOCAL_DT BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
  AND m.LINE_ITEM_ID IN (
    SELECT li.line_item_id
    FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item` li
    WHERE TO_JSON_STRING(li.pages) LIKE '%MAIN_SLIDER%'
  )
GROUP BY 1, 2, 3
ORDER BY prints DESC;

-- Resultado conocido: DEAL_TYPE = NULL en todos los registros MARKETING.
-- No se encontró ningún valor fallback/default/preloaded.
-- → FALLBACK no está modelado en la data actual.

-- V5b. ¿Existe algún campo sub_type o product_type con valor fallback?
SELECT DISTINCT sub_type, product_type, type, COUNT(*) AS cnt
FROM `meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item`
WHERE TO_JSON_STRING(pages) LIKE '%MAIN_SLIDER%'
GROUP BY 1, 2, 3
ORDER BY cnt DESC;
