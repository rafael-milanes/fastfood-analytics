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
# # Notebook: 03_extraccion_mongodb
# # Descripcion: Extrae colecciones desde MongoDB Atlas
# #              y las guarda en capa Bronze del Lakehouse
# # Autor: Rafael Milanes
# # Fecha: 2025-03
# # ============================================================


# CELL ********************

BRONZE_SENSORES = "Files/bronze/mongodb/ubicacion_sensores"
BRONZE_EVENTOS  = "Files/bronze/mongodb/sensor_eventos"

print("Rutas cargadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install pymongo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb+srv://user1:6SG5pdEUZGHbZwWC@cluster0.9ytpxrr.mongodb.net/?appName=Cluster0"
DB_NAME   = "test"

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# Extraer colecciones
sensores = list(db["Ubicacion_sensores"].find())
eventos  = list(db["sensor_eventos"].find())

print(f"Ubicacion_sensores: {len(sensores)} documentos")
print(f"sensor_eventos:     {len(eventos)} documentos")

client.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pymongo import MongoClient
import pandas as pd
import re

MONGO_URI = "mongodb+srv://user1:6SG5pdEUZGHbZwWC@cluster0.9ytpxrr.mongodb.net/?appName=Cluster0"
DB_NAME   = "test"

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

def limpiar_columnas(df):
    # Elimina caracteres invalidos en nombres de columnas para Delta
    df.columns = [re.sub(r'[;{}()\n\t=,]', '_', col).strip() for col in df.columns]
    return df

colecciones = {
    "Ubicacion_sensores" : "Files/bronze/mongodb/ubicacion_sensores",
    "sensor_eventos"     : "Files/bronze/mongodb/sensor_eventos",
}

for coleccion, path in colecciones.items():
    try:
        docs     = list(db[coleccion].find())
        df       = pd.DataFrame(docs)
        df       = df.drop(columns=["_id"], errors="ignore")
        df       = limpiar_columnas(df)
        spark_df = spark.createDataFrame(df)
        spark_df.write.format("delta").mode("overwrite").save(path)
        print(f"OK {coleccion}: {len(docs)} documentos, {df.shape[1]} columnas")
        print(f"   Columnas: {list(df.columns)}")
    except Exception as e:
        print(f"ERROR en {coleccion}: {e}")

client.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb+srv://user1:6SG5pdEUZGHbZwWC@cluster0.9ytpxrr.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["test"]

docs = list(db["sensor_eventos"].find())
client.close()

# Parsear el campo compuesto id;Sensor_id;valor;fecha
registros = []
for doc in docs:
    for key, value in doc.items():
        if key == "_id":
            continue
        # key = "id;Sensor_id;valor;fecha"
        # value = "5;1;1052.28;2024-01-01 04:00:00"
        columnas = key.split(";")
        valores  = str(value).split(";")
        if len(columnas) == len(valores):
            registros.append(dict(zip(columnas, valores)))

df_eventos = pd.DataFrame(registros)
print(f"Shape: {df_eventos.shape}")
print(df_eventos.head(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardar sensor_eventos forzando overwrite de schema
spark_df_eventos = spark.createDataFrame(df_eventos)
spark_df_eventos.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Files/bronze/mongodb/sensor_eventos")
print(f"OK sensor_eventos: {df_eventos.shape[0]} filas guardadas en Bronze")
print(f"Columnas: {list(df_eventos.columns)}")

# Verificacion final
print("\nVERIFICACION BRONZE MONGODB")
df_s = spark.read.format("delta").load("Files/bronze/mongodb/ubicacion_sensores")
df_e = spark.read.format("delta").load("Files/bronze/mongodb/sensor_eventos")
print(f"OK Ubicacion_sensores: {df_s.count()} filas")
print(f"OK sensor_eventos:     {df_e.count()} filas")
df_e.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
