# ML_MAIN_SLIDER · Dashboard de Gobernanza

Dashboard de visibilidad en tiempo real de todas las campañas activas sirviendo
en el placement `ML_MAIN_SLIDER`, separadas por cluster de prioridad.

---

## Estructura de archivos

```
ml-slider-dashboard/
├── main.sql        → query principal parametrizable (BigQuery)
├── validate.sql    → queries de validación por sección
├── dashboard.html  → dashboard estático (abrir directo en browser)
└── README.md       → este archivo
```

---

## Cómo correr main.sql

### Opción A — BigQuery Console (modo script)

1. Abrí `main.sql` en BigQuery Console.
2. Descomentá el bloque `DECLARE` al inicio.
3. Modificá los valores por defecto según necesidad:
   ```sql
   DECLARE site_id              STRING  DEFAULT 'MLA';
   DECLARE ros_filter           STRING  DEFAULT 'all';
   DECLARE time_window          STRING  DEFAULT 'today';
   DECLARE max_rows_per_cluster INT64   DEFAULT 20;
   ```
4. Reemplazá `@site_id` → `site_id`, `@ros_filter` → `ros_filter`, etc. en todo el archivo.
5. Ejecutá.

### Opción B — BigQuery API / cliente Python

```python
from google.cloud import bigquery

client = bigquery.Client(project="meli-bi-data")

query_params = [
    bigquery.ScalarQueryParameter("site_id",              "STRING", "MLA"),
    bigquery.ScalarQueryParameter("ros_filter",           "STRING", "all"),
    bigquery.ScalarQueryParameter("time_window",          "STRING", "today"),
    bigquery.ScalarQueryParameter("max_rows_per_cluster", "INT64",  20),
]

job_config = bigquery.QueryJobConfig(query_parameters=query_params)
results = client.query(open("main.sql").read(), job_config=job_config).to_dataframe()
print(results.to_json(orient="records"))
```

### Parámetros

| Parámetro              | Tipo   | Valores posibles                   | Default  |
|------------------------|--------|------------------------------------|----------|
| `@site_id`             | STRING | MLA, MLB, MLM, MLC, MLU, MCO, MPE | (req.)   |
| `@ros_filter`          | STRING | `all`, `ros_only`, `segmented_only`| `all`    |
| `@time_window`         | STRING | `today`, `last_7_days`             | `today`  |
| `@max_rows_per_cluster`| INT64  | cualquier entero positivo          | `20`     |

---

## Cómo generar el HTML con datos reales

1. Corré `main.sql` con los parámetros deseados.
2. Exportá el resultado como JSON desde BigQuery Console
   (botón "Save results" → "JSON (local file)").
3. Abrí `dashboard.html` en el browser.
4. Usá el botón **"Cargar JSON"** para cargar el archivo, o pegá el JSON
   en el textarea y hacé click en **"Renderizar"**.

### Alternativa: datos embebidos (sin interacción del usuario)

Para tener el dashboard auto-cargado, pegá el JSON directamente en el HTML:

```js
// En dashboard.html, línea final del <script>:
const EMBEDDED_DATA = [ /* pegá aquí el array JSON */ ];
```

Con `EMBEDDED_DATA.length > 0`, el dashboard renderiza automáticamente al abrir el archivo.

---

## Decisiones de diseño

### Tablas incluidas / excluidas

| Tabla       | Incluida | Motivo                                                                 |
|-------------|----------|------------------------------------------------------------------------|
| `campaign`  | ✅       | status, campaign_type, site_id, priority, tags, advertiser_id          |
| `line_item` | ✅       | pages (placement), start/end date (vigencia), audience_dmp (ROS)       |
| `BT_ADS_DISP_METRICS_DAILY` | ✅ | PRINTS_QTY, CLICKS_QTY, filtrado por LINE_ITEM_ID + site + fecha |
| `plan`      | ❌       | Solo contiene: plan_id, campaign_id, distribution_type, currency. Sin campos de vigencia. |
| `budget`    | ❌       | Tiene start/end date pero representan períodos de asignación presupuestaria, no scheduling de campaña. La vigencia ya está cubierta por li.start_date/end_date. |

### Criterio de "activo ahora"

```
campaign.status = 'active'
AND campaign_type IN ('PROGRAMMATIC', 'GUARANTEED')
AND campaign.tags IS NULL
AND campaign.site_id = @site_id
AND pages contiene ML_MAIN_SLIDER_(PRIMARY|HIGH|MEDIUM)
AND CURRENT_TIMESTAMP() BETWEEN li.start_date AND li.end_date
```

**Por qué `tags IS NULL`:**
`campaign.tags` es un JSON array de flags de sistema (`PAUSED_EXPIRED`, `DEPRECATED`,
`DESTINATION_CLOSED`, `ALL_LINE_ITEM_PAUSED`, etc.). `IS NULL` significa que la
campaña no tiene ningún flag desfavorable activo. Es la condición más segura para
filtrar campañas verdaderamente activas sin depender de un enum completo de valores.

### Clusters de prioridad

| Cluster            | Regla de mapeo                                   | Estado       |
|--------------------|--------------------------------------------------|--------------|
| PRIORIDAD_0        | `campaign_type = 'GUARANTEED'`                   | Condicional* |
| PRIORIDAD_PRINCIPAL| `slider_level = 'PRIMARY'`                       | ✅ Confirmado |
| PRIORIDAD_ALTA     | `slider_level = 'HIGH'`                          | ✅ Confirmado |
| PRIORIDAD_MEDIA    | `slider_level = 'MEDIUM'`                        | ✅ Confirmado |
| FALLBACK           | Sin campo real                                   | ❌ No disponible |

**\* PRIORIDAD_0 — Condicional:**
`campaign_type = 'GUARANTEED'` con `goal.strategy = 'guaranteed'` es el único indicador
real encontrado de reserva garantizada. Durante la exploración no se encontraron
line_items activos de tipo GUARANTEED en ML_MAIN_SLIDER, pero el campo existe en la
tabla y la lógica está incluida. Si existen, aparecerán en el dashboard; si no, la
sección muestra mensaje explicativo.

**FALLBACK — No disponible:**
- `DEAL_TYPE` = NULL en el 100% de los registros MARKETING en métricas.
- `sub_type = 'STANDARD'` y `product_type = 'DSP_SELFSERVICE'` en todas las muestras.
- Ningún campo con valor `fallback`, `default`, `preloaded` encontrado.
- Conclusión: FALLBACK no está modelado en la data actual.

### ROS vs Segmentado

**Definición auditada:**

```sql
is_ros = CASE
  WHEN audience_dmp IS NOT NULL                              THEN FALSE  -- segmento DMP
  WHEN TO_JSON_STRING(audience) NOT IN ('null','[]','')     THEN FALSE  -- audiencia custom
  WHEN TO_JSON_STRING(context)  NOT IN ('null','[]','')     THEN FALSE  -- targeting contextual
  WHEN TO_JSON_STRING(geolocation) NOT IN ('null','[]','')  THEN FALSE  -- targeting geo
  ELSE TRUE  -- ROS
END
```

**Evidencia:**
- En 30 muestras de line_items MAIN_SLIDER activos, `segmentation_strategy` fue
  **siempre NULL** → descartado como discriminador.
- `audience_dmp` mostró valores numéricos (IDs de segmento DMP: 490525, 358489, etc.)
  cuando hay segmentación, y NULL cuando no hay.
- `audience`, `context`, `geolocation` son fallbacks por si `audience_dmp` no cubre
  todos los casos de targeting.

### Métricas

- `impressions = SUM(PRINTS_QTY)` — no existe columna `impressions` en la tabla;
  `PRINTS_QTY` es el equivalente confirmado.
- `clicks = SUM(CLICKS_QTY)`
- `ctr = SAFE_DIVIDE(clicks, impressions)`
- `share_of_impressions = SAFE_DIVIDE(impressions, SUM(impressions) OVER (PARTITION BY cluster))`
- Join vía `LINE_ITEM_ID` — `PAGE_NAME` en métricas usa nombres cortos (`home`, `vpp`, etc.),
  no `ML_MAIN_SLIDER_*`. El join por LINE_ITEM_ID es el único método válido.

### Owner

- Campo usado: `campaign.advertiser_id` (ID numérico del anunciante).
- Alternativa disponible: `campaign.account_id`.
- No existe campo `owner` como tal en ninguna tabla. `advertiser_id` es el identificador
  más semántico del dueño de la campaña.

---

## Qué no se pudo / limitaciones

| Limitación                          | Motivo                                                    |
|-------------------------------------|-----------------------------------------------------------|
| PRIORIDAD_0 sin datos confirmados   | No hay line_items GUARANTEED activos en MAIN_SLIDER en el período explorado |
| FALLBACK no modelado                | Ningún campo con semántica de fallback/default en la data |
| `segmentation_strategy` inutilizable| Siempre NULL para line_items de MAIN_SLIDER               |
| Plan/budget sin uso                 | Sin campos de vigencia útiles (ver sección de decisiones) |
| No se puede hacer GROUP BY en JSON  | Campos audience_dmp, audience, context, geolocation son tipo JSON en BQ → se resuelve con TO_JSON_STRING() en subqueries |

---

## Timezones por site

| Site | Timezone                        |
|------|---------------------------------|
| MLA  | America/Argentina/Buenos_Aires  |
| MLB  | America/Sao_Paulo               |
| MLM  | America/Mexico_City             |
| MLC  | America/Santiago                |
| MLU  | America/Montevideo              |
| MCO  | America/Bogota                  |
| MPE  | America/Lima                    |
