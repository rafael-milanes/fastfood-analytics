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
# # PRUEBA TÉCNICA — FASTFOOD ANALYTICS
# # Notebook: 01_setup_lakehouse
# # Descripción: Crea la estructura de carpetas Bronze/Silver/Gold
# #              en el Lakehouse siguiendo arquitectura medallion
# # Autor: Rafael Milanés
# # Fecha: 2025-03
# # ============================================================


# CELL ********************

# Verificar contexto de Fabric
import notebookutils

# Listar el contenido actual del Lakehouse
mssparkutils.fs.ls("Files/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CAPA BRONZE — Datos crudos tal como llegan de las fuentes
# ============================================================

bronze_folders = [
    "Files/bronze/mysql/ventas",
    "Files/bronze/mysql/ticket",
    "Files/bronze/mysql/product",
    "Files/bronze/mysql/type",
    "Files/bronze/mysql/tiendas",
    "Files/bronze/mongodb/ubicacion_sensores",
    "Files/bronze/mongodb/sensor_eventos",
]

for folder in bronze_folders:
    mssparkutils.fs.mkdirs(folder)
    print(f"✅ Creada: {folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CAPA SILVER — Datos limpios y estandarizados
# ============================================================

silver_folders = [
    "Files/silver/ventas_clean",
    "Files/silver/sensores_clean",
    "Files/silver/tiendas_clean",
    "Files/silver/ventas_sensores_cruzado",  # JOIN tiendas + sensores más cercanos
]

for folder in silver_folders:
    mssparkutils.fs.mkdirs(folder)
    print(f"✅ Creada: {folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CAPA GOLD — Datos listos para Power BI y modelo predictivo
# ============================================================

gold_folders = [
    "Files/gold/fact_ventas",           # Tabla de hechos principal
    "Files/gold/dim_tiendas",           # Dimensión tiendas
    "Files/gold/dim_fecha",             # Dimensión fecha
    "Files/gold/dim_producto",          # Dimensión producto
    "Files/gold/dim_sector",            # Dimensión sector/zona
    "Files/gold/predicciones",          # Output del modelo predictivo
]

for folder in gold_folders:
    mssparkutils.fs.mkdirs(folder)
    print(f"✅ Creada: {folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Verificación final de la estructura creada
# ============================================================

def listar_estructura(path, nivel=0):
    items = mssparkutils.fs.ls(path)
    for item in items:
        prefijo = "  " * nivel + ("📁 " if item.isDir else "📄 ")
        print(f"{prefijo}{item.name}")
        if item.isDir:
            listar_estructura(item.path, nivel + 1)

print("🏗️ ESTRUCTURA DEL LAKEHOUSE — ARQUITECTURA MEDALLION\n")
print("=" * 50)
listar_estructura("Files/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rutas Bronze
BRONZE_VENTAS    = "Files/bronze/mysql/ventas"
BRONZE_TICKET    = "Files/bronze/mysql/ticket"
BRONZE_PRODUCT   = "Files/bronze/mysql/product"
BRONZE_TYPE      = "Files/bronze/mysql/type"
BRONZE_TIENDAS   = "Files/bronze/mysql/tiendas"
BRONZE_SENSORES  = "Files/bronze/mongodb/ubicacion_sensores"
BRONZE_EVENTOS   = "Files/bronze/mongodb/sensor_eventos"

# Rutas Silver
SILVER_VENTAS    = "Files/silver/ventas_clean"
SILVER_SENSORES  = "Files/silver/sensores_clean"
SILVER_TIENDAS   = "Files/silver/tiendas_clean"
SILVER_CRUZADO   = "Files/silver/ventas_sensores_cruzado"

# Rutas Gold
GOLD_FACT_VENTAS  = "Files/gold/fact_ventas"
GOLD_DIM_TIENDAS  = "Files/gold/dim_tiendas"
GOLD_DIM_FECHA    = "Files/gold/dim_fecha"
GOLD_DIM_PRODUCTO = "Files/gold/dim_producto"
GOLD_DIM_SECTOR   = "Files/gold/dim_sector"
GOLD_PREDICCIONES = "Files/gold/predicciones"

print("Variables de rutas cargadas correctamente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
