# Tarea 3 - Procesamiento de Datos Batch y en Tiempo Real con Apache Spark y Kafka
# ==============================================================================
# PROYECTO: Procesamiento Masivo Batch con Apache Spark (Datos Sisbén)
# AUTOR: Stiwer Andrés Castillo Jiménez
# CURSO: Big Data - UNAD
# ==============================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

def ejecutar_procesamiento_batch():
    # 1. Inicialización de la SparkSession optimizada para entornos distribuidos
    spark = SparkSession.builder \
        .appName("ProcesamientoBatch_Sisben_StiwerCastillo") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("==================================================")
    print("--- 1. INICIALIZACIÓN Y CARGA DE DATOS ---")
    print("==================================================")

    # Ruta del archivo masivo del Sisbén (más de 4.4 millones de registros)
    ruta_dataset = "query.csv"

    # 2. Carga del archivo CSV con inferencia de esquema, cabecera y 
    # manejo robusto de comillas dobles y caracteres especiales
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(ruta_dataset)

    total_inicial = df_raw.count()
    print(f">> Total de registros iniciales cargados: {total_inicial}")

    # Impresión del esquema estructural detectado por Spark
    print("\nEsquema Estructural del Dataset:")
    df_raw.printSchema()

    print("==================================================")
    print("--- 2. LIMPIEZA Y FILTRADO DE DATOS ---")
    print("==================================================")

    # 3. Limpieza de valores nulos críticos para garantizar la calidad analítica
    df_limpio = df_raw.dropna(subset=["cod_mpio", "grupo"])
    total_limpio = df_limpio.count()
    print(f">> Total de registros después de limpieza y filtrado: {total_limpio}")

    print("==================================================")
    print("--- 3. ANÁLISIS EXPLORATORIO DE DATOS (EDA) ---")
    print("==================================================")

    # 4. Agregación 1: Distribución espacial de registros por Código de Municipio (Top 10)
    print("\n>> Top 10 Municipios con mayor concentración de registros:")
    df_limpio.groupBy("cod_mpio") \
        .agg(count("*").alias("total_registros")) \
        .orderBy(desc("total_registros")) \
        .show(10, truncate=False)

    # 5. Agregación 2: Segmentación poblacional por Grupos del Sisbén (A, B, C, D)
    print("\n>> Distribución Poblacional por Grupos Socioeconómicos:")
    df_limpio.groupBy("grupo") \
        .agg(count("*").alias("total_registros")) \
        .orderBy("grupo") \
        .show(truncate=False)

    print("==================================================")
    print("--- 4. ESTADÍSTICAS DESCRIPTIVAS (MÉTRICAS CLAVE) ---")
    print("==================================================")

    # 6. Cálculo matemático descriptivo acotado a columnas clave para visualización limpia
    columnas_analisis = ["cod_mpio", "nivel", "fex"]
    columnas_validas = [c for c in columnas_analisis if c in df_limpio.columns]

    if columnas_validas:
        df_limpio.select(columnas_validas).describe().show(truncate=False)
    else:
        df_limpio.select(df_limpio.columns[:3]).describe().show(truncate=False)

    print("\n>> Proceso Batch finalizado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    ejecutar_procesamiento_batch()

## Descripción del proyecto

Este proyecto desarrolla un ejercicio integral de procesamiento de datos utilizando **Apache Spark**, **PySpark** y **Apache Kafka** sobre una máquina virtual con entorno Linux. 

Se emplea un conjunto de datos masivo del **Sisbén** (Sistema de Identificación de Potenciales Beneficiarios de Programas Sociales) obtenido desde la plataforma de Datos Abiertos de Colombia (`[datos.gov.co](https://www.datos.gov.co/Estad-sticas-Nacionales/DNP-Sisb-n-Personas/hq2v-5umk/about_data)`), conteniendo más de 4.4 millones de registros. 

El objetivo principal es implementar dos arquitecturas clave de Big Data:
1. **Procesamiento Batch (Por lotes):** Carga, limpieza, análisis exploratorio de datos (EDA) y cálculo de estadísticas descriptivas sobre el volumen masivo de registros.
2. **Procesamiento en Tiempo Real (Streaming):** Configuración de un *topic* en Kafka y un consumidor en Spark Structured Streaming para simular y procesar flujos de datos dinámicos al vuelo.

---

## Estructura del Repositorio

* **`LÉAME.md`** : Documentación principal del proyecto e instrucciones de uso.
* **`batch_tarea3.py`** : Script de PySpark encargado de la carga masiva del archivo CSV del Sisbén, limpieza de nulos, agrupaciones por municipio/grupos y estadísticas descriptivas.
* **`spark_streaming_consumer.py`** : Script de Spark Structured Streaming para la lectura y procesamiento en tiempo real conectado a un clúster de Kafka.

---

## Tecnologías utilizadas

* Python 3.x
* Apache Spark (v3.5.x)
* PySpark
* Apache Kafka (v3.6.x)
* Ubuntu Linux (Entorno Virtual VBox `BIGDATA`)

---

## Instrucciones de Ejecución

### 1. Procesamiento en Batch

1. Asegúrate de contar con el archivo de datos masivos `query.csv` (Sisbén) en el directorio de trabajo de tu entorno.
2. Ejecuta el script de procesamiento por lotes mediante el comando de Spark:
   ```bash
   spark-submit batch_tarea3.py

2. Procesamiento en Tiempo Real (Streaming & Kafka)
Inicia los servicios de Zookeeper y Kafka en la máquina virtual.

Ejecuta el productor de eventos simulados para alimentar el topic de Kafka:

Bash
python3 kafka_producer.py
En otra terminal, ejecuta la aplicación consumidora de Spark Structured Streaming con el paquete conector correspondiente:

Bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
Visualiza en la consola la actualización dinámica de los micro-batches en tiempo real.

Consideraciones metodológicas
El conjunto de datos analizado en batch contiene información socioeconómica a gran escala, requiriendo operaciones distribuidas para evitar cuellos de botella en memoria. Los resultados reflejan la distribución poblacional y las métricas estadísticas correspondientes al procesamiento eficiente ejecutado por el clúster de Spark.

Desarrollado por:
Stiwer Andrés Castillo Jiménez

Ingeniería de Sistemas - Universidad Nacional Abierta y a Distancia (UNAD)

Actividad correspondiente a la Tarea 3: Procesamiento de Datos con Apache Spark y Kafka.
