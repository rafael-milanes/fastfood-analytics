# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6b801e2-f509-4a7f-b935-070f904e8fbb",
# META       "default_lakehouse_name": "LH_FastFood",
# META       "default_lakehouse_workspace_id": "a11ffcd2-afad-43a8-9783-c5e21a790b93",
# META       "known_lakehouses": [
# META         {
# META           "id": "9be7fe1b-a1d9-4820-bec3-a90c8f99ed77"
# META         },
# META         {
# META           "id": "a6b801e2-f509-4a7f-b935-070f904e8fbb"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ============================================================
# # PRUEBA TECNICA - FASTFOOD ANALYTICS
# # Notebook: 04_silver_transformacion
# # Descripcion: Limpieza, transformacion y cruce de datos
# #              Tiendas + Sensores usando distancia Haversine
# # Autor: Rafael Milanes
# # Fecha: 2025-03
# # ============================================================


# CELL ********************

# Bronze
BRONZE_TIENDAS  = "Files/bronze/mysql/tiendas"
BRONZE_VENTAS   = "Files/bronze/mysql/ventas"
BRONZE_TICKET   = "Files/bronze/mysql/ticket"
BRONZE_PRODUCT  = "Files/bronze/mysql/product"
BRONZE_TYPE     = "Files/bronze/mysql/type"
BRONZE_REGION   = "Files/bronze/mysql/region"
BRONZE_SENSORES = "Files/bronze/mongodb/ubicacion_sensores"
BRONZE_EVENTOS  = "Files/bronze/mongodb/sensor_eventos"

# Silver
SILVER_TIENDAS  = "Files/silver/tiendas_clean"
SILVER_VENTAS   = "Files/silver/ventas_clean"
SILVER_SENSORES = "Files/silver/sensores_clean"
SILVER_CRUZADO  = "Files/silver/ventas_sensores_cruzado"

print("Rutas cargadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# Leer todas las tablas Bronze
df_tiendas  = spark.read.format("delta").load(BRONZE_TIENDAS)
df_ventas   = spark.read.format("delta").load(BRONZE_VENTAS)
df_ticket   = spark.read.format("delta").load(BRONZE_TICKET)
df_product  = spark.read.format("delta").load(BRONZE_PRODUCT)
df_type     = spark.read.format("delta").load(BRONZE_TYPE)
df_region   = spark.read.format("delta").load(BRONZE_REGION)
df_sensores = spark.read.format("delta").load(BRONZE_SENSORES)
df_eventos  = spark.read.format("delta").load(BRONZE_EVENTOS)

print("Tablas cargadas desde Bronze:")
print(f"  Tiendas:  {df_tiendas.count()} filas")
print(f"  Ventas:   {df_ventas.count()} filas")
print(f"  Ticket:   {df_ticket.count()} filas")
print(f"  Sensores: {df_sensores.count()} filas")
print(f"  Eventos:  {df_eventos.count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== TIENDAS ===")
df_tiendas.printSchema()
df_tiendas.show(3)

print("=== SENSORES ===")
df_sensores.printSchema()
df_sensores.show(5)

print("=== EVENTOS ===")
df_eventos.printSchema()
df_eventos.show(3)

print("=== VENTAS ===")
df_ventas.printSchema()
df_ventas.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Parsear coordenadas de tiendas desde string "lat, lon"
df_tiendas_clean = df_tiendas \
    .withColumn("lat", F.split(F.col("ubicacion"), ",")[0].cast(DoubleType())) \
    .withColumn("lon", F.split(F.col("ubicacion"), ",")[1].cast(DoubleType())) \
    .dropna(subset=["lat", "lon"])

df_tiendas_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(SILVER_TIENDAS)

print(f"OK Tiendas Silver: {df_tiendas_clean.count()} filas")
df_tiendas_clean.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parsear coordenadas de sensores
df_sensores_clean = df_sensores \
    .withColumn("lat", F.split(F.col("ubicacion"), ",")[0].cast(DoubleType())) \
    .withColumn("lon", F.split(F.col("ubicacion"), ",")[1].cast(DoubleType())) \
    .dropna(subset=["lat", "lon"])

df_sensores_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(SILVER_SENSORES)

print(f"OK Sensores Silver: {df_sensores_clean.count()} filas")
df_sensores_clean.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import DoubleType, IntegerType, TimestampType

df_eventos_clean = df_eventos \
    .withColumn("id",        F.col("id").cast(IntegerType())) \
    .withColumn("Sensor_id", F.col("Sensor_id").cast(IntegerType())) \
    .withColumn("valor",     F.col("valor").cast(DoubleType())) \
    .withColumn("fecha",     F.to_timestamp(F.col("fecha"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna()

print(f"OK Eventos clean: {df_eventos_clean.count()} filas")
df_eventos_clean.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Funcion distancia Haversine en km
def haversine(lat1, lon1, lat2, lon2):
    try:
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
            math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    except:
        return None

haversine_udf = udf(haversine, DoubleType())

# Cruzar todas las tiendas con todos los sensores
df_cruce = df_tiendas_clean.alias("t") \
    .crossJoin(df_sensores_clean.alias("s")) \
    .withColumn("distancia_km", haversine_udf(
        F.col("t.lat"), F.col("t.lon"),
        F.col("s.lat"), F.col("s.lon")
    ))

# Quedarse con el sensor mas cercano por tienda
from pyspark.sql.window import Window

window = Window.partitionBy("t.id").orderBy("distancia_km")
df_sensor_cercano = df_cruce \
    .withColumn("rank", F.rank().over(window)) \
    .filter(F.col("rank") == 1) \
    .select(
        F.col("t.id").alias("tienda_id"),
        F.col("t.ubicacion").alias("tienda_ubicacion"),
        F.col("t.lat").alias("tienda_lat"),
        F.col("t.lon").alias("tienda_lon"),
        F.col("s.id").alias("sensor_id"),
        F.col("s.name").alias("sensor_nombre"),
        F.col("distancia_km")
    )

print(f"OK Cruce tienda-sensor: {df_sensor_cercano.count()} filas")
df_sensor_cercano.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Precipitacion promedio por sensor por dia
df_precip_diaria = df_eventos_clean \
    .withColumn("fecha_dia", F.to_date(F.col("fecha"))) \
    .groupBy("Sensor_id", "fecha_dia") \
    .agg(
        F.avg("valor").alias("precip_promedio"),
        F.max("valor").alias("precip_max"),
        F.min("valor").alias("precip_min")
    ).withColumnRenamed("Sensor_id", "sensor_id_precip") \
     .withColumnRenamed("fecha_dia", "fecha_dia_precip")

# Ventas + ticket + tienda + sensor + precipitacion
df_ventas_sensor = df_ventas \
    .join(df_ticket, df_ventas.factura_id == df_ticket.factura_id, "left") \
    .withColumn("fecha_dia", F.to_date(F.col("fecha_venta"))) \
    .join(df_sensor_cercano,
        df_ventas.tienda_id == df_sensor_cercano.tienda_id, "left") \
    .join(df_precip_diaria,
        (F.col("sensor_id") == F.col("sensor_id_precip")) &
        (F.col("fecha_dia") == F.col("fecha_dia_precip")), "left") \
    .select(
        df_ventas.id.alias("venta_id"),
        df_ventas.tienda_id,
        df_ventas.factura_id,
        F.col("fecha_venta"),
        F.col("fecha_dia"),
        F.col("product_id"),
        F.col("tipo_compra_id"),
        F.col("sensor_id"),
        F.col("sensor_nombre"),
        F.col("distancia_km"),
        F.col("precip_promedio"),
        F.col("precip_max"),
        F.col("precip_min")
    )

df_ventas_sensor.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(SILVER_CRUZADO)

print(f"OK Silver cruzado: {df_ventas_sensor.count()} filas")
df_ventas_sensor.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
