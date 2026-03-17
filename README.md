# 🍔 FastFood Analytics
## Análisis de Impacto de Precipitaciones en Ventas — Microsoft Fabric

**Autor:** Rafael Milanés Hernández  
**Certificaciones:** Microsoft PL-300 | Microsoft DP-600 (Fabric Analytics Engineer Associate)  
**Plataforma:** Microsoft Fabric (Trial) — Workspace: `FastFood_Analytics`  
**Fecha:** Marzo 2026

---

## 🎯 Objetivo

Evaluar si existe una relación significativa entre las precipitaciones (lluvias) 
en los distintos sectores de Bogotá y el nivel de ventas en las tiendas de FastFood, 
construyendo un pipeline de datos end-to-end con modelo predictivo e integración 
de fuentes heterogéneas (MySQL relacional + MongoDB documental).

---

## 🏗️ Arquitectura — Medallion en Microsoft Fabric
```
┌─────────────────┐     ┌─────────────────┐
│  MySQL (Aiven)  │     │  MongoDB Atlas  │
│  7 tablas       │     │  2 colecciones  │
│  host: mysql-   │     │  DB: test       │
│  3b4106d0-      │     │  cluster0.      │
│  etraining...   │     │  9ytpxrr.mongo  │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│           🥉 BRONZE (Delta)             │
│  Datos crudos sin transformación        │
│  Files/bronze/mysql/   (7 tablas)       │
│  Files/bronze/mongodb/ (2 colecciones)  │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│           🥈 SILVER (Delta)             │
│  Limpieza + casteo + cruce Haversine    │
│  ventas_sensores_cruzado: 208,863 filas │
│  Precipitación promedio diaria por      │
│  sensor más cercano a cada tienda       │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│           🥇 GOLD (Delta Tables)        │
│  Star schema + predicciones RF          │
│  fact_ventas: 208,863 filas             │
│  6 tablas registradas como SQL Tables   │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌──────────────────┐
        │  Power BI Service│
        │  Direct Lake     │
        │  SQL Endpoint    │
        └──────────────────┘
```

---

## 📦 Fuentes de Datos

### MySQL — Aiven Cloud
```
Host:     mysql-3b4106d0-etraining-62c0.g.aivencloud.com
Port:     10185
Database: FastFood
```

| Tabla | Filas | Descripción |
|---|---|---|
| `Ticket` | 208,863 | Transacciones con fecha, producto y tipo de compra |
| `Ventas` | 85,936 | Relación venta-tienda-factura |
| `Product` | 20 | Catálogo de productos (nombre en español) |
| `Tiendas` | 10 | Ubicación geográfica, región y tamaño |
| `Type` | 2 | Tipos de venta: Presencial / En línea |
| `Region` | 4 | Regiones: Este, Norte, Oeste, Sur |
| `Size` | 3 | Tamaños de tienda |

### MongoDB Atlas
```
Cluster: cluster0.9ytpxrr.mongodb.net
DB:      test  (nota: la documentación indica "Prueba_Tecnica" pero los 
               datos reales están en la DB "test")
```

| Colección | Documentos | Estructura |
|---|---|---|
| `Ubicacion_sensores` | 20 | `{id, name, ubicacion (lat,lon), region_id}` |
| `sensor_eventos` | 175,200 | Campo único `id;Sensor_id;valor;fecha` separado por `;` |

> ⚠️ **Nota técnica:** Los documentos de `sensor_eventos` llegaron con un campo 
> compuesto `"id;Sensor_id;valor;fecha": "5;1;1052.28;2024-01-01 04:00:00"` 
> — se implementó un parser manual con `split(";")` para normalizar las 4 columnas.

---

## 📓 Notebooks — Flujo ETL

### `01_setup_estructura.ipynb`
Crea la estructura de carpetas Bronze/Silver/Gold en el Lakehouse usando 
`mssparkutils.fs.mkdirs()`. Define las rutas como variables reutilizables 
en todos los notebooks.
```python
Files/
├── bronze/
│   ├── mysql/     (ventas, ticket, product, type, tiendas, region, size)
│   └── mongodb/   (ubicacion_sensores, sensor_eventos)
├── silver/
│   ├── ventas_clean/
│   ├── sensores_clean/
│   ├── tiendas_clean/
│   └── ventas_sensores_cruzado/
└── gold/
    ├── fact_ventas/
    ├── dim_tiendas/
    ├── dim_fecha/
    ├── dim_producto/
    ├── dim_sector/
    └── predicciones/
```

### `02_extraccion_mysql.ipynb`
Conexión a MySQL vía **SQLAlchemy + PyMySQL** (se migró de `pd.read_sql` 
con pymysql directo por warnings de compatibilidad).
```python
engine = create_engine(
    f"mysql+pymysql://user1:****@mysql-3b4106d0...:{10185}/FastFood",
    connect_args={"ssl": {"ssl_disabled": False}}
)
```

Guarda cada tabla en Bronze como **Delta** con:
```python
spark_df.write.format("delta").mode("overwrite").save(path)
```

### `03_extraccion_mongodb.ipynb`
Conexión vía **PyMongo**. Identificación de que los datos reales están 
en la DB `test` (no `Prueba_Tecnica` como indica la documentación).

Parser personalizado para `sensor_eventos`:
```python
columnas = key.split(";")   # ["id", "Sensor_id", "valor", "fecha"]
valores  = str(value).split(";")  # ["5", "1", "1052.28", "2024-01-01 04:00:00"]
registros.append(dict(zip(columnas, valores)))
```

Se usó `.option("overwriteSchema", "true")` para sobreescribir el schema 
Delta previo con estructura incorrecta.

### `04_silver_transformacion.ipynb`

**Parseo de coordenadas:**
```python
df_tiendas_clean = df_tiendas \
    .withColumn("lat", F.split(F.col("ubicacion"), ",")[0].cast(DoubleType())) \
    .withColumn("lon", F.split(F.col("ubicacion"), ",")[1].cast(DoubleType()))
```

**Cruce geográfico Haversine** — asigna a cada tienda el sensor más cercano:
```python
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
```

CrossJoin tiendas × sensores → ranking por distancia → sensor rank=1 por tienda.  
**Distancia promedio tienda-sensor: 1.31 km** — valida representatividad del dato.

**Precipitación diaria por sensor:**
```python
df_precip_diaria = df_eventos_clean \
    .withColumn("fecha_dia", F.to_date(F.col("fecha"))) \
    .groupBy("Sensor_id", "fecha_dia") \
    .agg(F.avg("valor").alias("precip_promedio"), ...)
```

**Silver final cruzado: 208,863 filas** con columnas:
`venta_id, tienda_id, factura_id, fecha_venta, fecha_dia, product_id, 
tipo_compra_id, sensor_id, sensor_nombre, distancia_km, precip_promedio, 
precip_max, precip_min`

### `05_gold_modelo_predictivo.ipynb`

**Star schema Gold:**
```
                    gold_dim_fecha
                         │ fecha_dia
                         │
gold_dim_sector ─── gold_fact_ventas ─── gold_dim_tiendas
tipo_compra_id │    (208,863 filas)  │   tienda_id
               │         │           │
               │    producto_id      │
               │         │           └── gold_predicciones
          gold_dim_producto              tienda_id
```

**Modelo predictivo Random Forest:**
```python
features = ["tienda_id", "region_id", "tamano_id", "tipo_compra_id",
            "precip_promedio", "precip_max", "dia_semana", "mes"]
target   = "num_ventas"  # conteo de ventas por tienda por día

model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
```

**Métricas:**
- MAE (Mean Absolute Error): calculado en test set (20%)
- R²: score de ajuste del modelo
- Error Predicción %: 53.3% — ver análisis en sección Conclusiones

**Predicciones guardadas:** 7,273 filas en `gold_predicciones` con columnas:
`tienda_id, fecha_dia, precip_promedio, precip_max, dia_semana, mes, 
ventas_reales, ventas_predichas`

---

## 📊 Dashboard Power BI

**Modelo semántico:** `SM_FastFood_Analytics` (Direct Lake — SQL Endpoint)  
**Reporte:** `RPT_FastFood_Analytics`

**Medidas DAX creadas en tabla `_Medidas`:**
```dax
Cantidad Ventas = COUNTROWS(gold_fact_ventas)

Ventas Reales Total = SUM(gold_predicciones[ventas_reales])

Ventas Random Forest = SUM(gold_predicciones[ventas_predichas])

Precipitacion Promedio = AVERAGE(gold_fact_ventas[precip_promedio])

Distancia Sensor Promedio = AVERAGE(gold_fact_ventas[distancia_km])

Ventas Presencial = CALCULATE([Total Ventas],
    gold_dim_sector[tipo_compra_nombre] = "Presencial")

Ventas En Linea = CALCULATE([Total Ventas],
    gold_dim_sector[tipo_compra_nombre] = "En línea")

Error Prediccion % = DIVIDE(
    ABS([Ventas Reales Total] - [Ventas Predichas Total]),
    [Ventas Reales Total], 0) * 100

```
**Parámetro de campo — tabla `Parámetro`:**
```dax
Parámetro = {
    ("dia_semana", NAMEOF('gold_dim_fecha'[dia_semana]), 0),
    ("mes-año",    NAMEOF('gold_dim_fecha'[fecha_dia]),  1)
}
```
Permite al usuario del reporte alternar dinámicamente el eje X del gráfico 
de ventas entre vista por **día de la semana** y vista por **mes-año**, 
sin necesidad de duplicar el visual. Es una técnica avanzada de Power BI 
que mejora la experiencia del usuario y reduce el número de visuales en 
el lienzo.

**Relaciones del modelo (todas *:1):**
- `gold_fact_ventas[fecha_dia]` → `gold_dim_fecha[fecha_dia]`
- `gold_fact_ventas[producto_id]` → `gold_dim_producto[producto_id]`
- `gold_fact_ventas[tienda_id]` → `gold_dim_tiendas[tienda_id]`
- `gold_fact_ventas[tipo_compra_id]` → `gold_dim_sector[tipo_compra_id]`
- `gold_predicciones[tienda_id]` → `gold_dim_tiendas[tienda_id]`

---

## 🔍 Conclusiones del Análisis

**1. Correlación lluvia-ventas confirmada**
Las regiones Este y Norte (precipitación 1,099-1,100 mm) registran el mayor 
volumen de ventas (~52,500 transacciones). Sur y Oeste (1,025-1,042 mm) muestran 
ventas más bajas (~52,252 y ~51,806). La relación es positiva y consistente 
en las 4 regiones.

**2. Canal de venta equilibrado**
Ventas presenciales (~27K) y en línea (~26K) son prácticamente iguales por región, 
indicando que la lluvia no desplaza significativamente al consumidor hacia el 
canal digital — contrario a lo que podría esperarse.

**3. Productos estrella**
Papas fritas (49K) y Refrescos (43K) concentran el 44% de las ventas totales, 
independientemente de las condiciones climáticas y región.

**4. Patrón semanal**
Las ventas caen el miércoles (día 4) en todas las regiones y repuntan hacia 
el fin de semana — oportunidad de implementar promociones de mitad de semana.

**5. Modelo predictivo — diagnóstico y mejoras propuestas**
El Random Forest captura correctamente la dirección de la relación lluvia-ventas. 
El error del 53.3% se explica por:
- **Diferencia de escala:** precipitación en 1,000+ mm vs ventas en 50-200 por día
- **Features limitadas:** solo clima, región y temporalidad — faltan precio, 
  promociones, eventos y días festivos
- **Agregación diaria:** el modelo trabaja con promedios, perdiendo variabilidad intradiaria

**Próximas iteraciones recomendadas:**
- Normalización/estandarización de variables antes del entrenamiento
- Incorporación de features adicionales (precio, eventos, festivos)
- Evaluación de modelos de series de tiempo: **Prophet** o **LSTM** para 
  capturar mejor la estacionalidad semanal y mensual detectada

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología | Versión/Detalle |
|---|---|---|
| Plataforma | Microsoft Fabric | Trial — 13 días |
| Almacenamiento | OneLake — Delta | Formato Delta Lake |
| Ingesta MySQL | SQLAlchemy + PyMySQL | SSL habilitado |
| Ingesta MongoDB | PyMongo | Atlas cluster |
| Procesamiento | PySpark | Fabric runtime |
| Cruce geográfico | Haversine (Python UDF) | Distancia en km |
| Modelo predictivo | scikit-learn | RandomForestRegressor |
| Visualización | Power BI Service | Direct Lake mode |
| Arquitectura datos | Medallion | Bronze/Silver/Gold |

---

## 📁 Estructura del Repositorio
```
fastfood-analytics/
├── 📓 01_setup_estructura.ipynb
├── 📓 02_extraccion_mysql.ipynb
├── 📓 03_extraccion_mongodb.ipynb
├── 📓 04_silver_transformacion.ipynb
├── 📓 05_gold_modelo_predictivo.ipynb
└── 📄 README.md
```
