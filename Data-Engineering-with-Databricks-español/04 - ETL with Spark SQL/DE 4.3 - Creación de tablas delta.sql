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
-- MAGIC # Creación de tablas delta
-- MAGIC 
-- MAGIC Después de extraer datos de fuentes de datos externas, cárguelos en Lakehouse para garantizar que todos los beneficios de la plataforma Databricks se puedan aprovechar por completo.
-- MAGIC 
-- MAGIC Si bien las diferentes organizaciones pueden tener diferentes políticas sobre cómo se cargan inicialmente los datos en Databricks, normalmente recomendamos que las primeras tablas representen una versión sin procesar de los datos, y que la validación y el enriquecimiento ocurran en etapas posteriores. Este patrón garantiza que, incluso si los datos no coinciden con las expectativas con respecto a los tipos de datos o los nombres de las columnas, no se descartará ningún dato, lo que significa que la intervención programática o manual aún puede recuperar los datos en un estado parcialmente dañado o no válido.
-- MAGIC 
-- MAGIC Esta lección se centrará principalmente en el patrón utilizado para crear la mayoría de las tablas, instrucciones **`CREATE TABLE _ AS SELECT`** (CTAS).
-- MAGIC 
-- MAGIC ## Objetivos de aprendizaje
-- MAGIC Al final de esta lección, debería ser capaz de:
-- MAGIC - Use declaraciones CTAS para crear tablas de Delta Lake
-- MAGIC - Crear nuevas tablas a partir de vistas o tablas existentes
-- MAGIC - Enriquecer los datos cargados con metadatos adicionales
-- MAGIC - Declarar esquema de tabla con columnas generadas y comentarios descriptivos.
-- MAGIC - Establezca opciones avanzadas para controlar la ubicación de los datos, la aplicación de la calidad y la partición

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Ejecutar la instalación
-- MAGIC 
-- MAGIC El script de configuración creará los datos y declarará los valores necesarios para que se ejecute el resto de este cuaderno.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC 
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC 
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.
-- MAGIC 
-- MAGIC CTAS statements also do not support specifying additional file options.
-- MAGIC 
-- MAGIC We can see how this would present significant limitations when trying to ingest data from CSV files.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/raw/sales-csv/`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC To correctly ingest this data to a Delta Lake table, we'll need to use a reference to the files that allows us to specify options.
-- MAGIC 
-- MAGIC In the previous lesson, we showed doing this by registering an external table. Here, we'll slightly evolve this syntax to specify the options to a temporary view, and then use this temp view as the source for a CTAS statement to successfully register the Delta table.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Filtering and Renaming Columns from Existing Tables
-- MAGIC 
-- MAGIC Simple transformations like changing column names or omitting columns from target tables can be easily accomplished during table creation.
-- MAGIC 
-- MAGIC The following statement creates a new table containing a subset of columns from the **`sales`** table. 
-- MAGIC 
-- MAGIC Here, we'll presume that we're intentionally leaving out information that potentially identifies the user or that provides itemized purchase details. We'll also rename our fields with the assumption that a downstream system has different naming conventions than our source data.
-- MAGIC 
-- MAGIC ## Filtrado y cambio de nombre de columnas de tablas existentes
-- MAGIC 
-- MAGIC Las transformaciones simples, como cambiar los nombres de las columnas u omitir columnas de las tablas de destino, se pueden lograr fácilmente durante la creación de tablas.
-- MAGIC 
-- MAGIC La siguiente instrucción crea una nueva tabla que contiene un subconjunto de columnas de la tabla **`sales`**.
-- MAGIC 
-- MAGIC En este caso, supondremos que estamos omitiendo intencionalmente información que potencialmente identifica al usuario o que proporciona detalles de compra detallados. También cambiaremos el nombre de nuestros campos asumiendo que un sistema descendente tiene convenciones de nomenclatura diferentes a las de nuestros datos de origen.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Note that we could have accomplished this same goal with a view, as shown below.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ## Declare Schema with Generated Columns
-- MAGIC 
-- MAGIC As noted previously, CTAS statements do not support schema declaration. We note above that the timestamp column appears to be some variant of a Unix timestamp, which may not be the most useful for our analysts to derive insights. This is a situation where generated columns would be beneficial.
-- MAGIC 
-- MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table (introduced in DBR 8.3).
-- MAGIC 
-- MAGIC The code below demonstrates creating a new table while:
-- MAGIC 1. Specifying column names and types
-- MAGIC 1. Adding a <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">generated column</a> to calculate the date
-- MAGIC 1. Providing a descriptive column comment for the generated column
-- MAGIC 
-- MAGIC ## Declarar esquema con columnas generadas
-- MAGIC 
-- MAGIC Como se señaló anteriormente, las declaraciones de CTAS no admiten la declaración de esquema. Notamos anteriormente que la columna de marca de tiempo parece ser una variante de una marca de tiempo de Unix, que puede no ser la más útil para que nuestros analistas obtengan información. Esta es una situación en la que las columnas generadas serían beneficiosas.
-- MAGIC 
-- MAGIC Las columnas generadas son un tipo especial de columna cuyos valores se generan automáticamente en función de una función especificada por el usuario sobre otras columnas en la tabla Delta (introducida en DBR 8.3).
-- MAGIC 
-- MAGIC El siguiente código demuestra la creación de una nueva tabla mientras:
-- MAGIC 1. Especificación de nombres y tipos de columnas
-- MAGIC 1. Agregar una <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">columna generada</a> para calcular la fecha
-- MAGIC 1. Proporcionar un comentario de columna descriptivo para la columna generada

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC 
-- MAGIC Because **`date`** is a generated column, if we write to **`purchase_dates`** without providing values for the **`date`** column, Delta Lake automatically computes them.
-- MAGIC 
-- MAGIC **NOTE**: The cell below configures a setting to allow for generating columns when using a Delta Lake **`MERGE`** statement. We'll see more on this syntax later in the course.
-- MAGIC 
-- MAGIC Debido a que **`date`** es una columna generada, si escribimos en **`purchase_dates`** sin proporcionar valores para la columna **`date`**, Delta Lake los calcula automáticamente.
-- MAGIC 
-- MAGIC **NOTA**: La celda a continuación configura una configuración para permitir la generación de columnas cuando se utiliza una instrucción **`MERGE`** de Delta Lake. Veremos más sobre esta sintaxis más adelante en el curso.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC We can see below that all dates were computed correctly as data was inserted, although neither our source data or insert query specified the values in this field.
-- MAGIC 
-- MAGIC As with any Delta Lake source, the query automatically reads the most recent snapshot of the table for any query; you never need to run **`REFRESH TABLE`**.

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC It's important to note that if a field that would otherwise be generated is included in an insert to a table, this insert will fail if the value provided does not exactly match the value that would be derived by the logic used to define the generated column.
-- MAGIC 
-- MAGIC We can see this error by uncommenting and running the cell below:
-- MAGIC 
-- MAGIC Es importante tener en cuenta que si un campo que de otro modo se generaría se incluye en una inserción en una tabla, esta inserción fallará si el valor proporcionado no coincide exactamente con el valor que derivaría de la lógica utilizada para definir la columna generada.
-- MAGIC 
-- MAGIC Podemos ver este error descomentando y ejecutando la siguiente celda:

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Add a Table Constraint
-- MAGIC 
-- MAGIC The error message above refers to a **`CHECK constraint`**. Generated columns are a special implementation of check constraints.
-- MAGIC 
-- MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
-- MAGIC 
-- MAGIC Databricks currently support two types of constraints:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
-- MAGIC 
-- MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
-- MAGIC 
-- MAGIC Below, we'll add a **`CHECK`** constraint to the **`date`** column of our table. Note that **`CHECK`** constraints look like standard **`WHERE`** clauses you might use to filter a dataset.
-- MAGIC 
-- MAGIC ## Agregar una restricción de tabla
-- MAGIC 
-- MAGIC El mensaje de error anterior se refiere a una restricción **`CHECK`**. Las columnas generadas son una implementación especial de las restricciones de verificación.
-- MAGIC 
-- MAGIC Debido a que Delta Lake aplica el esquema al escribir, Databricks puede admitir cláusulas de administración de restricciones de SQL estándar para garantizar la calidad y la integridad de los datos agregados a una tabla.
-- MAGIC 
-- MAGIC Los databricks actualmente admiten dos tipos de restricciones:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** restricciones</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** restricciones</a>
-- MAGIC 
-- MAGIC En ambos casos, debe asegurarse de que no haya datos que violen la restricción en la tabla antes de definir la restricción. Una vez que se ha agregado una restricción a una tabla, los datos que violen la restricción darán como resultado un error de escritura.
-- MAGIC 
-- MAGIC A continuación, agregaremos una restricción **`CHECK`** a la columna **`date`** de nuestra tabla. Tenga en cuenta que las restricciones **`CHECK`** parecen cláusulas estándar **`WHERE`** que podría usar para filtrar un conjunto de datos.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Table constraints are shown in the **`TBLPROPERTIES`** field.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Enrich Tables with Additional Options and Metadata
-- MAGIC 
-- MAGIC So far we've only scratched the surface as far as the options for enriching Delta Lake tables.
-- MAGIC 
-- MAGIC Below, we show evolving a CTAS statement to include a number of additional configurations and metadata.
-- MAGIC 
-- MAGIC Our **`SELECT`** clause leverages two built-in Spark SQL commands useful for file ingestion:
-- MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
-- MAGIC * **`input_file_name()`** records the source data file for each record in the table
-- MAGIC 
-- MAGIC We also include logic to create a new date column derived from timestamp data in the source.
-- MAGIC 
-- MAGIC The **`CREATE TABLE`** clause contains several options:
-- MAGIC * A **`COMMENT`** is added to allow for easier discovery of table contents
-- MAGIC * A **`LOCATION`** is specified, which will result in an external (rather than managed) table
-- MAGIC * The table is **`PARTITIONED BY`** a date column; this means that the data from each data will exist within its own directory in the target storage location
-- MAGIC 
-- MAGIC **NOTE**: Partitioning is shown here primarily to demonstrate syntax and impact. Most Delta Lake tables (especially small-to-medium sized data) will not benefit from partitioning. Because partitioning physically separates data files, this approach can result in a small files problem and prevent file compaction and efficient data skipping. The benefits observed in Hive or HDFS do not translate to Delta Lake, and you should consult with an experienced Delta Lake architect before partitioning tables.
-- MAGIC 
-- MAGIC **As a best practice, you should default to non-partitioned tables for most use cases when working with Delta Lake.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC The metadata fields added to the table provide useful information to understand when records were inserted and from where. This can be especially helpful if troubleshooting problems in the source data becomes necessary.
-- MAGIC 
-- MAGIC All of the comments and properties for a given table can be reviewed using **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC 
-- MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.
-- MAGIC 
-- MAGIC Los campos de metadatos agregados a la tabla brindan información útil para comprender cuándo se insertaron los registros y desde dónde. Esto puede ser especialmente útil si es necesario solucionar problemas en los datos de origen.
-- MAGIC 
-- MAGIC Todos los comentarios y propiedades de una tabla determinada se pueden revisar mediante **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC 
-- MAGIC **NOTA**: Delta Lake agrega automáticamente varias propiedades de tabla en la creación de tablas.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC Listing the location used for the table reveals that the unique values in the partition column **`first_touch_date`** are used to create data directories.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Cloning Delta Lake Tables
-- MAGIC Delta Lake has two options for efficiently copying Delta Lake tables.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.
-- MAGIC 
-- MAGIC ## Clonación de tablas del lago Delta
-- MAGIC Delta Lake tiene dos opciones para copiar tablas de Delta Lake de manera eficiente.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** copia completamente los datos y metadatos de una tabla de origen a un destino. Esta copia se produce de forma incremental, por lo que ejecutar este comando nuevamente puede sincronizar los cambios desde la ubicación de origen a la de destino.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Because all the data files must be copied over, this can take quite a while for large datasets.
-- MAGIC 
-- MAGIC If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, **`SHALLOW CLONE`** can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.
-- MAGIC 
-- MAGIC Debido a que todos los archivos de datos deben copiarse, esto puede llevar bastante tiempo para grandes conjuntos de datos.
-- MAGIC 
-- MAGIC Si desea crear una copia de una tabla rápidamente para probar la aplicación de cambios sin el riesgo de modificar la tabla actual, **`SHALLOW CLONE`** puede ser una buena opción. Los clones superficiales simplemente copian los registros de transacciones de Delta, lo que significa que los datos no se mueven.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC In either case, data modifications applied to the cloned version of the table will be tracked and stored separately from the source. Cloning is a great way to set up tables for testing SQL code while still in development.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC 
-- MAGIC In this notebook, we focused primarily on DDL and syntax for creating Delta Lake tables. In the next notebook, we'll explore options for writing updates to tables.

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
