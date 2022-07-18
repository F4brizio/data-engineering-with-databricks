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
-- MAGIC # Proporcionar opciones para fuentes externas
-- MAGIC Si bien la consulta directa de archivos funciona bien para formatos autodescriptivos, muchas fuentes de datos requieren configuraciones adicionales o declaración de esquema para ingerir registros correctamente.
-- MAGIC 
-- MAGIC En esta lección, crearemos tablas utilizando fuentes de datos externas. Si bien estas tablas aún no se almacenarán en el formato Delta Lake (y, por lo tanto, no se optimizarán para Lakehouse), esta técnica ayuda a facilitar la extracción de datos de diversos sistemas externos.
-- MAGIC 
-- MAGIC ## Objetivos de aprendizaje
-- MAGIC Al final de esta lección, debería ser capaz de:
-- MAGIC - Use Spark SQL para configurar opciones para extraer datos de fuentes externas
-- MAGIC - Cree tablas contra fuentes de datos externas para varios formatos de archivo
-- MAGIC - Describir el comportamiento predeterminado al consultar tablas definidas en fuentes externas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Cuando las consultas directas no funcionan
-- MAGIC 
-- MAGIC Si bien las vistas se pueden usar para persistir consultas directas en archivos entre sesiones, este enfoque tiene una utilidad limitada.
-- MAGIC 
-- MAGIC Los archivos CSV son uno de los formatos de archivo más comunes, pero una consulta directa contra estos archivos rara vez arroja los resultados deseados.

-- COMMAND ----------

SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Podemos ver de lo anterior que:
-- MAGIC 1. La fila del encabezado se extrae como una fila de la tabla
-- MAGIC 1. Todas las columnas se cargan como una sola columna
-- MAGIC 1. El archivo está delimitado por barras verticales (**`|`**)
-- MAGIC 1. La columna final parece contener datos anidados que se truncan

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Registro de tablas en datos externos con opciones de lectura
-- MAGIC 
-- MAGIC Si bien Spark extraerá algunas fuentes de datos de autodescripción de manera eficiente utilizando la configuración predeterminada, muchos formatos requerirán la declaración del esquema u otras opciones.
-- MAGIC 
-- MAGIC Si bien hay muchas <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">configuraciones adicionales</a> que puede establecer al crear tablas en fuentes externas, la siguiente sintaxis demuestra los elementos esenciales necesarios para extraer datos de la mayoría de los formatos.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC Tenga en cuenta que las opciones se pasan con claves como texto sin comillas y valores entre comillas. Spark admite muchas <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">fuentes de datos</a> con opciones personalizadas y los sistemas adicionales pueden tener soporte no oficial a través de <a href="https://docs.databricks.com/libraries/index.html" target="_blank">bibliotecas</a> externas.
-- MAGIC 
-- MAGIC **NOTA**: Dependiendo de la configuración de su espacio de trabajo, es posible que necesite asistencia del administrador para cargar bibliotecas y configurar los ajustes de seguridad necesarios para algunas fuentes de datos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC La siguiente celda muestra el uso de Spark SQL DDL para crear una tabla en una fuente CSV externa, especificando:
-- MAGIC 1. Los nombres y tipos de columnas
-- MAGIC 1. El formato de archivo
-- MAGIC 1. El delimitador utilizado para separar campos
-- MAGIC 1. La presencia de un encabezado
-- MAGIC 1. La ruta a donde se almacenan estos datos

-- COMMAND ----------

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que no se ha movido ningún dato durante la declaración de la tabla. Similar a cuando consultamos directamente nuestros archivos y creamos una vista, todavía estamos apuntando a archivos almacenados en una ubicación externa.
-- MAGIC 
-- MAGIC Ejecute la siguiente celda para confirmar que los datos ahora se están cargando correctamente.

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Todos los metadatos y opciones pasados durante la declaración de la tabla se conservarán en el metastore, lo que garantiza que los datos en la ubicación siempre se leerán con estas opciones.
-- MAGIC 
-- MAGIC **NOTA**: Cuando se trabaja con archivos CSV como fuente de datos, es importante asegurarse de que el orden de las columnas no cambie si se agregan archivos de datos adicionales al directorio de origen. Debido a que el formato de datos no tiene una fuerte aplicación de esquema, Spark cargará columnas y aplicará nombres de columna y tipos de datos en el orden especificado durante la declaración de la tabla.
-- MAGIC 
-- MAGIC Ejecutar **`DESCRIBE EXTENDED`** en una tabla mostrará todos los metadatos asociados con la definición de la tabla.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Límites de tablas con fuentes de datos externas
-- MAGIC 
-- MAGIC Si tomó otros cursos sobre Databricks o revisó la literatura de nuestra compañía, es posible que haya oído hablar de Delta Lake y Lakehouse. Tenga en cuenta que cada vez que definamos tablas o consultas en fuentes de datos externas, **no podemos** esperar las garantías de rendimiento asociadas con Delta Lake y Lakehouse.
-- MAGIC 
-- MAGIC Por ejemplo: si bien las tablas de Delta Lake garantizarán que siempre consulte la versión más reciente de sus datos de origen, las tablas registradas con otras fuentes de datos pueden representar versiones anteriores en caché.
-- MAGIC 
-- MAGIC La celda a continuación ejecuta alguna lógica que podemos pensar que simplemente representa un sistema externo que actualiza directamente los archivos subyacentes a nuestra tabla.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("sales_csv")
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(f"{DA.paths.working_dir}/sales-csv"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Si observamos el recuento actual de registros en nuestra tabla, el número que vemos no reflejará estas filas recién insertadas.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC En el momento en que consultamos previamente esta fuente de datos, Spark almacenó automáticamente en caché los datos subyacentes en el almacenamiento local. Esto garantiza que, en las consultas posteriores, Spark proporcione el rendimiento óptimo simplemente consultando este caché local.
-- MAGIC 
-- MAGIC Nuestra fuente de datos externa no está configurada para decirle a Spark que debe actualizar estos datos.
-- MAGIC 
-- MAGIC **Podemos** actualizar manualmente el caché de nuestros datos ejecutando el comando **`REFRESH TABLE`**.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que actualizar nuestra tabla invalidará nuestro caché, lo que significa que necesitaremos volver a escanear nuestra fuente de datos original y recuperar todos los datos en la memoria.
-- MAGIC 
-- MAGIC Para conjuntos de datos muy grandes, esto puede llevar una cantidad significativa de tiempo.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extracción de datos de bases de datos SQL
-- MAGIC Las bases de datos SQL son una fuente de datos extremadamente común y Databricks tiene un controlador JDBC estándar para conectarse con muchas versiones de SQL.
-- MAGIC 
-- MAGIC La sintaxis general para crear estas conexiones es:
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC In the code sample below, we'll connect with <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>.
-- MAGIC   
-- MAGIC 
-- MAGIC   
-- MAGIC   **NOTA:** SQLite usa un archivo local para almacenar una base de datos y no requiere un puerto, nombre de usuario o contraseña.
-- MAGIC   
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **ADVERTENCIA**: La configuración de back-end del servidor JDBC supone que está ejecutando este portátil en un clúster de un solo nodo. Si está ejecutando en un clúster con varios trabajadores, el cliente que se ejecuta en los ejecutores no podrá conectarse al controlador.

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Now we can query this table as if it were defined locally.

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Mirar los metadatos de la tabla revela que hemos capturado la información del esquema del sistema externo. Las propiedades de almacenamiento (que incluirían el nombre de usuario y la contraseña asociados con la conexión) se redactan automáticamente.

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Si bien la tabla aparece como **`MANAGED`**, enumerar el contenido de la ubicación especificada confirma que no se conservan datos localmente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbc_users_path = f"{DA.paths.user_db}/users_jdbc/"
-- MAGIC print(jdbc_users_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(jdbc_users_path)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que algunos sistemas SQL, como los almacenes de datos, tendrán controladores personalizados. Spark interactuará con varias bases de datos externas de manera diferente, pero los dos enfoques básicos se pueden resumir como:
-- MAGIC 1. Mover la(s) tabla(s) de origen completa(s) a Databricks y luego ejecutar la lógica en el clúster actualmente activo
-- MAGIC 1. Empujar la consulta a la base de datos SQL externa y solo transferir los resultados a Databricks
-- MAGIC 
-- MAGIC En cualquier caso, trabajar con conjuntos de datos muy grandes en bases de datos SQL externas puede generar una sobrecarga significativa debido a:
-- MAGIC 1. Latencia de transferencia de red asociada con el movimiento de todos los datos a través de la Internet pública
-- MAGIC 1. Ejecución de lógica de consulta en sistemas de origen no optimizados para consultas de big data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Ejecute la siguiente celda para eliminar las tablas y archivos asociados con esta lección.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
