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
# # Notebook: 05_gold_modelo_predictivo
# # Descripcion: Construye tablas Gold y modelo predictivo
# #              que estima ventas en funcion de precipitacion
# # Autor: Rafael Milanes
# # Fecha: 2025-03
# # ============================================================


# CELL ********************

SILVER_CRUZADO    = "Files/silver/ventas_sensores_cruzado"
BRONZE_PRODUCT    = "Files/bronze/mysql/product"
BRONZE_TYPE       = "Files/bronze/mysql/type"
BRONZE_TIENDAS    = "Files/bronze/mysql/tiendas"
BRONZE_REGION     = "Files/bronze/mysql/region"

GOLD_FACT_VENTAS  = "Files/gold/fact_ventas"
GOLD_DIM_TIENDAS  = "Files/gold/dim_tiendas"
GOLD_DIM_FECHA    = "Files/gold/dim_fecha"
GOLD_DIM_PRODUCTO = "Files/gold/dim_producto"
GOLD_DIM_SECTOR   = "Files/gold/dim_sector"
GOLD_PREDICCIONES = "Files/gold/predicciones"

print("Rutas cargadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_silver    = spark.read.format("delta").load(SILVER_CRUZADO)
df_product   = spark.read.format("delta").load(BRONZE_PRODUCT)
df_type      = spark.read.format("delta").load(BRONZE_TYPE)
df_tiendas   = spark.read.format("delta").load(BRONZE_TIENDAS)
df_region    = spark.read.format("delta").load(BRONZE_REGION)

print(f"Silver cargado: {df_silver.count()} filas")
df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dim Producto
df_dim_producto = df_product.select(
    F.col("id").alias("producto_id"),
    F.col("nombre").alias("producto_nombre")
)
df_dim_producto.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_DIM_PRODUCTO)
print(f"OK dim_producto: {df_dim_producto.count()} filas")

# Dim Tiendas
df_dim_tiendas = df_tiendas.join(df_region,
    df_tiendas.region_id == df_region.id, "left") \
    .select(
        df_tiendas.id.alias("tienda_id"),
        df_region.id.alias("region_id"),
        F.col("nombre").alias("region_nombre"),
        df_tiendas.tamano_id,
        df_tiendas.empleados,
        df_tiendas.ubicacion
    )
df_dim_tiendas.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_DIM_TIENDAS)
print(f"OK dim_tiendas: {df_dim_tiendas.count()} filas")

# Dim Sector (tipo compra)
df_dim_sector = df_type.select(
    F.col("id").alias("tipo_compra_id"),
    F.col("tipo").alias("tipo_compra_nombre")
)
df_dim_sector.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_DIM_SECTOR)
print(f"OK dim_sector: {df_dim_sector.count()} filas")

# Dim Fecha
df_dim_fecha = df_silver.select("fecha_dia").distinct() \
    .withColumn("anio",       F.year("fecha_dia")) \
    .withColumn("mes",        F.month("fecha_dia")) \
    .withColumn("dia",        F.dayofmonth("fecha_dia")) \
    .withColumn("dia_semana", F.dayofweek("fecha_dia")) \
    .withColumn("trimestre",  F.quarter("fecha_dia"))
df_dim_fecha.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_DIM_FECHA)
print(f"OK dim_fecha: {df_dim_fecha.count()} filas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_ventas = df_silver \
    .join(df_dim_tiendas, "tienda_id", "left") \
    .select(
        F.col("venta_id"),
        F.col("tienda_id"),
        F.col("factura_id"),
        F.col("fecha_dia"),
        F.col("product_id").alias("producto_id"),
        F.col("tipo_compra_id"),
        F.col("sensor_id"),
        F.col("sensor_nombre"),
        F.col("distancia_km"),
        F.col("precip_promedio"),
        F.col("precip_max"),
        F.col("precip_min"),
        F.col("region_id"),
        F.col("region_nombre"),
        F.col("tamano_id"),
        F.col("empleados")
    )

df_fact_ventas.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_FACT_VENTAS)

print(f"OK fact_ventas Gold: {df_fact_ventas.count()} filas")
df_fact_ventas.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Agregar por tienda y dia: contar ventas y promedio precipitacion
df_modelo = df_fact_ventas \
    .groupBy("tienda_id", "fecha_dia", "region_id",
             "tamano_id", "tipo_compra_id",
             "precip_promedio", "precip_max") \
    .agg(F.count("venta_id").alias("num_ventas")) \
    .dropna()

# Agregar variables temporales
df_modelo = df_modelo \
    .withColumn("dia_semana", F.dayofweek("fecha_dia")) \
    .withColumn("mes",        F.month("fecha_dia"))

pdf = df_modelo.toPandas()
print(f"Dataset modelo: {pdf.shape}")
print(pdf.head(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import numpy as np

features = ["tienda_id", "region_id", "tamano_id", "tipo_compra_id",
            "precip_promedio", "precip_max", "dia_semana", "mes"]
target   = "num_ventas"

pdf_clean = pdf[features + [target]].dropna()

X = pdf_clean[features]
y = pdf_clean[target]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

print(f"MAE:  {mean_absolute_error(y_test, y_pred):.2f}")
print(f"R2:   {r2_score(y_test, y_pred):.4f}")
print(f"\nImportancia de variables:")
for feat, imp in sorted(zip(features, model.feature_importances_),
                         key=lambda x: -x[1]):
    print(f"  {feat}: {imp:.4f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf_clean["ventas_predichas"] = model.predict(X)
pdf_clean["ventas_reales"]    = y.values

df_pred = spark.createDataFrame(pdf_clean[[
    "tienda_id", "fecha_dia" if "fecha_dia" in pdf_clean.columns else "mes",
    "precip_promedio", "ventas_reales", "ventas_predichas"
]])

# Merge fecha_dia de vuelta
pdf_full = pdf_clean.copy()
pdf_full["fecha_dia"] = pdf["fecha_dia"].values
df_pred = spark.createDataFrame(pdf_full[[
    "tienda_id", "fecha_dia", "precip_promedio",
    "precip_max", "dia_semana", "mes",
    "ventas_reales", "ventas_predichas"
]])

df_pred.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save(GOLD_PREDICCIONES)

print(f"OK predicciones Gold: {df_pred.count()} filas guardadas")
df_pred.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("VERIFICACION GOLD\n")
tablas_gold = {
    "fact_ventas"  : GOLD_FACT_VENTAS,
    "dim_tiendas"  : GOLD_DIM_TIENDAS,
    "dim_fecha"    : GOLD_DIM_FECHA,
    "dim_producto" : GOLD_DIM_PRODUCTO,
    "dim_sector"   : GOLD_DIM_SECTOR,
    "predicciones" : GOLD_PREDICCIONES,
}

for tabla, path in tablas_gold.items():
    try:
        df_check = spark.read.format("delta").load(path)
        print(f"OK {tabla}: {df_check.count()} filas")
    except Exception as e:
        print(f"ERROR {tabla}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Registrar tablas Gold como tablas SQL en el Lakehouse
tablas_registrar = {
    "gold_fact_ventas"  : GOLD_FACT_VENTAS,
    "gold_dim_tiendas"  : GOLD_DIM_TIENDAS,
    "gold_dim_fecha"    : GOLD_DIM_FECHA,
    "gold_dim_producto" : GOLD_DIM_PRODUCTO,
    "gold_dim_sector"   : GOLD_DIM_SECTOR,
    "gold_predicciones" : GOLD_PREDICCIONES,
}

for nombre, path in tablas_registrar.items():
    df_temp = spark.read.format("delta").load(path)
    df_temp.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(nombre)
    print(f"OK {nombre} registrada como tabla SQL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
