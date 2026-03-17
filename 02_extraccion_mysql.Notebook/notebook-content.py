# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9be7fe1b-a1d9-4820-bec3-a90c8f99ed77",
# META       "default_lakehouse_name": "LH_FastFood",
# META       "default_lakehouse_workspace_id": "8cac73dd-06dd-4ba7-ae25-a8edbcb324b3",
# META       "known_lakehouses": [
# META         {
# META           "id": "9be7fe1b-a1d9-4820-bec3-a90c8f99ed77"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ============================================================
# # PRUEBA TECNICA - FASTFOOD ANALYTICS
# # Notebook: 02_extraccion_mysql
# # Descripcion: Extrae tablas desde MySQL (Aiven Cloud)
# #              y las guarda en capa Bronze del Lakehouse
# # Autor: Rafael Milanes
# # Fecha: 2025-03
# # ============================================================


# CELL ********************

BRONZE_VENTAS   = "Files/bronze/mysql/ventas"
BRONZE_TICKET   = "Files/bronze/mysql/ticket"
BRONZE_PRODUCT  = "Files/bronze/mysql/product"
BRONZE_TYPE     = "Files/bronze/mysql/type"
BRONZE_TIENDAS  = "Files/bronze/mysql/tiendas"

print("Rutas cargadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install pymysql cryptography

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pymysql
import pandas as pd

HOST     = "mysql-3b4106d0-etraining-62c0.g.aivencloud.com"
PORT     = 10185
DATABASE = "FastFood"
USER     = "user1"
PASSWORD = "AVNS_FB2xcAz2en7pG0lHIsS"

def get_connection():
    return pymysql.connect(
        host=HOST,
        port=PORT,
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        ssl={"ssl_disabled": False}
    )

# Probar conexion
try:
    conn = get_connection()
    print("Conexion exitosa a MySQL")
    conn.close()
except Exception as e:
    print(f"Error de conexion: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sqlalchemy import create_engine, text

engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}",
    connect_args={"ssl": {"ssl_disabled": False}}
)

with engine.connect() as conn:
    result = conn.execute(text("SHOW TABLES"))
    tablas = [row[0] for row in result]

print("Tablas disponibles en FastFood:")
for t in tablas:
    print(f"  - {t}")
print(f"\nTotal: {len(tablas)} tablas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tablas = {
    "Product" : "Files/bronze/mysql/product",
    "Ticket"  : "Files/bronze/mysql/ticket",
    "Tiendas" : "Files/bronze/mysql/tiendas",
    "Type"    : "Files/bronze/mysql/type",
    "Ventas"  : "Files/bronze/mysql/ventas",
    "Region"  : "Files/bronze/mysql/region",
    "Size"    : "Files/bronze/mysql/size",
}

for tabla, path in tablas.items():
    try:
        df = pd.read_sql(f"SELECT * FROM {tabla}", engine)
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").save(path)
        print(f"OK {tabla}: {df.shape[0]} filas, {df.shape[1]} columnas")
    except Exception as e:
        print(f"ERROR en {tabla}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("VERIFICACION BRONZE MYSQL\n")
for tabla, path in tablas.items():
    try:
        df_check = spark.read.format("delta").load(path)
        print(f"OK {tabla}: {df_check.count()} filas en Bronze")
    except Exception as e:
        print(f"ERROR {tabla}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
