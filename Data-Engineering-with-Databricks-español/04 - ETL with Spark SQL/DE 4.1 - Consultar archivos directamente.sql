-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Extracción de datos directamente de archivos
-- MAGIC 
-- MAGIC En este notebook, aprenderá a extraer datos directamente de archivos mediante Spark SQL en Databricks.
-- MAGIC 
-- MAGIC Varios formatos de archivo admiten esta opción, pero es más útil para formatos de datos autodescriptivos (como parquet y JSON).
-- MAGIC 
-- MAGIC ## Objetivos de aprendizaje
-- MAGIC Al final de esta lección, debería ser capaz de:
-- MAGIC - Use Spark SQL para consultar directamente archivos de datos
-- MAGIC - Aproveche los métodos **`text`** y **`binaryFile`** para revisar el contenido de los archivos sin procesar

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Resumen de datos
-- MAGIC 
-- MAGIC En este ejemplo, trabajaremos con una muestra de datos sin procesar de Kafka escritos como archivos JSON.
-- MAGIC 
-- MAGIC Cada archivo contiene todos los registros consumidos durante un intervalo de 5 segundos, almacenados con el esquema completo de Kafka como un archivo JSON de varios registros.
-- MAGIC 
-- MAGIC | field | type | description |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | El campo **`user_id`** se utiliza como clave; este es un campo alfanumérico único que corresponde a información de sesión/cookie |
-- MAGIC | value | BINARY | Esta es la carga útil de datos completa (que se analizará más adelante), enviada como JSON |
-- MAGIC | topic | STRING | Si bien el servicio de Kafka aloja varios temas, aquí solo se incluyen los registros del tema **`clickstream`** |
-- MAGIC | partition | INTEGER | Nuestra implementación actual de Kafka usa solo 2 particiones (0 y 1) |
-- MAGIC | offset | LONG | Este es un valor único, que aumenta monótonamente para cada partición |
-- MAGIC | timestamp | LONG | Esta marca de tiempo se registra en milisegundos desde la época y representa el momento en que el productor agrega un registro a una partición |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que nuestro directorio de origen contiene muchos archivos JSON.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = f"{DA.paths.datasets}/raw/events-kafka"
-- MAGIC print(dataset_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Aquí, usaremos rutas de archivo relativas a los datos que se escribieron en la raíz DBFS.
-- MAGIC 
-- MAGIC La mayoría de los flujos de trabajo requerirán que los usuarios accedan a los datos desde ubicaciones externas de almacenamiento en la nube.
-- MAGIC 
-- MAGIC En la mayoría de las empresas, un administrador del espacio de trabajo será responsable de configurar el acceso a estas ubicaciones de almacenamiento.
-- MAGIC 
-- MAGIC Las instrucciones para configurar y acceder a estas ubicaciones se pueden encontrar en los cursos de autoaprendizaje específicos del proveedor de la nube titulados "Arquitectura de la nube e integración de sistemas".

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Query a Single File
-- MAGIC 
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC 
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC 
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path.

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that our preview displays all 321 rows of our source file.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Query a Directory of Files
-- MAGIC 
-- MAGIC Assuming all of the files in a directory have the same format and schema, all files can be queried simultaneously by specifying the directory path rather than an individual file.

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC By default, this query will only show the first 1000 rows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create References to Files
-- MAGIC This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.
-- MAGIC 
-- MAGIC When we create a view from a query against a path, we can reference this view in later queries. Here, we'll create a temporary view, but you can also create a permanent reference with regular view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;

SELECT * FROM events_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extract Text Files as Raw Strings
-- MAGIC 
-- MAGIC When working with text-based files (which include JSON, CSV, TSV, and TXT formats), you can use the **`text`** format to load each line of the file as a row with one string column named **`value`**. This can be useful when data sources are prone to corruption and custom text parsing functions will be used to extract value from text fields.

-- COMMAND ----------

SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extract the Raw Bytes and Metadata of a File
-- MAGIC 
-- MAGIC Some workflows may require working with entire files, such as when dealing with images or unstructured data. Using **`binaryFile`** to query a directory will provide file metadata alongside the binary representation of the file contents.
-- MAGIC 
-- MAGIC Specifically, the fields created will indicate the **`path`**, **`modificationTime`**, **`length`**, and **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
