# Tarea 3 - Procesamiento de Datos Batch y en Tiempo Real con Apache Spark y Kafka

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
