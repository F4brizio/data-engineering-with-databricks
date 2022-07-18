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
-- MAGIC # Funciones avanzadas de Delta Lake
-- MAGIC 
-- MAGIC Ahora que se siente cómodo realizando tareas básicas de datos con Delta Lake, podemos analizar algunas características exclusivas de Delta Lake.
-- MAGIC 
-- MAGIC Tenga en cuenta que, si bien algunas de las palabras clave que se usan aquí no forman parte de ANSI SQL estándar, todas las operaciones de Delta Lake se pueden ejecutar en Databricks mediante SQL.
-- MAGIC 
-- MAGIC ## Objetivos de aprendizaje
-- MAGIC Al final de esta lección, debería ser capaz de:
-- MAGIC * Use **`OPTIMIZE`** para compactar archivos pequeños
-- MAGIC * Use **`ZORDER`** indexar tablas
-- MAGIC * Describir la estructura de directorios de los archivos de Delta Lake
-- MAGIC * Revisar un historial de transacciones de mesa
-- MAGIC * Consultar y retroceder a la versión anterior de la tabla
-- MAGIC * Limpie los archivos de datos obsoletos con **`VACUUM`**
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC Lo primero que vamos a hacer es ejecutar un script de instalación. Definirá un username, userhome y una base de datos cuyo alcance sea para cada usuario.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Creación de una tabla Delta con historial
-- MAGIC 
-- MAGIC La siguiente celda condensa todas las transacciones de la lección anterior en una sola celda. (Excepto por el **`DROP TABLE`**!)
-- MAGIC 
-- MAGIC Mientras espera que se ejecute esta consulta, vea si puede identificar el número total de transacciones que se están ejecutando.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Examinar los detalles de la tabla
-- MAGIC 
-- MAGIC Databricks usa una Hive metastore de forma predeterminada para registrar bases de datos, tablas y vistas.
-- MAGIC 
-- MAGIC Usando **`DESCRIBE EXTENDED`** nos permite ver metadatos importantes sobre nuestra tabla.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **`DESCRIBE DETAIL`** es otro comando que nos permite explorar los metadatos de la tabla.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta el campo **`Location`**.
-- MAGIC 
-- MAGIC Si bien hasta ahora hemos estado pensando en nuestra tabla como una entidad relacional dentro de una base de datos, una tabla de Delta Lake en realidad está respaldada por una colección de archivos almacenados en el almacenamiento de objetos en la nube.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Explore los archivos de Delta Lake
-- MAGIC 
-- MAGIC Podemos ver los archivos que respaldan nuestra tabla de Delta Lake mediante una función de utilidades de Databricks.
-- MAGIC 
-- MAGIC **NOTA**: No es importante en este momento saber todo acerca de estos archivos para trabajar con Delta Lake, pero lo ayudará a obtener una mayor apreciación de cómo se implementa la tecnología.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que nuestro directorio contiene varios archivos de datos de Parquet y un directorio llamado **`_delta_log`**.
-- MAGIC 
-- MAGIC Los registros en las tablas de Delta Lake se almacenan como datos en archivos de Parquet.
-- MAGIC 
-- MAGIC Las transacciones en las tablas de Delta Lake se registran en **`_delta_log`**.
-- MAGIC 
-- MAGIC Podemos echar un vistazo dentro de **`_delta_log`** para ver más.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Each transaction results in a new JSON file being written to the Delta Lake transaction log. Here, we can see that there are 8 total transactions against this table (Delta Lake is 0 indexed).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Razonamiento sobre archivos de datos
-- MAGIC 
-- MAGIC Acabamos de ver muchos archivos de datos para lo que obviamente es una tabla muy pequeña.
-- MAGIC 
-- MAGIC **`DESCRIBE DETAIL`** nos permite ver algunos otros detalles sobre nuestra tabla Delta, incluida la cantidad de archivos.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Aquí vemos que nuestra tabla actualmente contiene 4 archivos de datos en su versión actual. Entonces, ¿qué hacen todos esos otros archivos de Parquet en nuestro directorio de tablas?
-- MAGIC 
-- MAGIC En lugar de sobrescribir o eliminar inmediatamente los archivos que contienen datos modificados, Delta Lake usa el registro de transacciones para indicar si los archivos son válidos o no en una versión actual de la tabla.
-- MAGIC 
-- MAGIC Aquí, veremos el registro de transacciones correspondiente a la instrucción **`MERGE`** anterior, donde se insertaron, actualizaron y eliminaron los registros.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC La columna **`add`** contiene una lista de todos los archivos nuevos escritos en nuestra tabla; la columna **`remove`** indica aquellos archivos que ya no deberían estar incluidos en nuestra tabla.
-- MAGIC 
-- MAGIC Cuando consultamos una tabla de Delta Lake, el motor de consulta utiliza los registros de transacciones para resolver todos los archivos que son válidos en la versión actual e ignora todos los demás archivos de datos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Compactación de archivos pequeños e indexación
-- MAGIC 
-- MAGIC Los archivos pequeños pueden ocurrir por una variedad de razones; en nuestro caso, realizamos una serie de operaciones en las que solo se insertaban uno o varios registros.
-- MAGIC 
-- MAGIC Los archivos se combinarán para obtener un tamaño óptimo (a escala según el tamaño de la tabla) mediante el comando **`OPTIMIZE`**.
-- MAGIC 
-- MAGIC **`OPTIMIZE`** reemplazará los archivos de datos existentes al combinar registros y reescribir los resultados.
-- MAGIC 
-- MAGIC Al ejecutar **`OPTIMIZE`**, los usuarios pueden opcionalmente especificar uno o varios campos para indexar **`ZORDER`**. Si bien la matemática específica del orden Z no es importante, acelera la recuperación de datos cuando se filtran los campos proporcionados colocando datos con valores similares dentro de los archivos de datos.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Dado lo pequeños que son nuestros datos, **`ZORDER`** no proporciona ningún beneficio, pero podemos ver todas las métricas que resultan de esta operación.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Revisión de transacciones de Delta Lake
-- MAGIC 
-- MAGIC Debido a que todos los cambios en la tabla de Delta Lake se almacenan en el registro de transacciones, podemos revisar fácilmente el <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">historial de la tabla</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Como era de esperar, **`OPTIMIZE`** creó otra versión de nuestra tabla, lo que significa que la versión 8 es nuestra versión más actual.
-- MAGIC 
-- MAGIC ¿Recuerda todos esos archivos de datos adicionales que se marcaron como eliminados en nuestro registro de transacciones? Estos nos brindan la posibilidad de consultar versiones anteriores de nuestra tabla.
-- MAGIC 
-- MAGIC Estas consultas de viaje en el tiempo se pueden realizar especificando la versión entera o una marca de tiempo.
-- MAGIC 
-- MAGIC **NOTA**: En la mayoría de los casos, usará una marca de tiempo para recrear datos en un momento de interés. Para nuestra demostración, usaremos la versión, ya que esto es determinista (mientras que puede ejecutar esta demostración en cualquier momento en el futuro).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Lo que es importante tener en cuenta sobre el viaje en el tiempo es que no estamos recreando un estado anterior de la tabla al deshacer las transacciones en nuestra versión actual; más bien, solo estamos consultando todos los archivos de datos que se indicaron como válidos a partir de la versión especificada.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Rollback Versions
-- MAGIC 
-- MAGIC Suponga que está escribiendo una consulta para eliminar manualmente algunos registros de una tabla y accidentalmente ejecuta esta consulta en el siguiente estado.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que cuando vemos un **`-1`** para la cantidad de filas afectadas por una eliminación, esto significa que se eliminó un directorio completo de datos.
-- MAGIC 
-- MAGIC Confirmemos esto a continuación.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Eliminar todos los registros de su tabla probablemente no sea el resultado deseado. Afortunadamente, podemos simplemente deshacer este compromiso.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que un comando **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> se registra como una transacción; no podrá ocultar por completo el hecho de que eliminó accidentalmente todos los registros de la tabla, pero podrá deshacer la operación y devolver la tabla al estado deseado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Limpieza de archivos obsoletos
-- MAGIC 
-- MAGIC Databricks limpiará automáticamente los archivos obsoletos en las tablas de Delta Lake.
-- MAGIC 
-- MAGIC Si bien el control de versiones y el viaje en el tiempo de Delta Lake son excelentes para consultar versiones recientes y revertir consultas, mantener los archivos de datos para todas las versiones de tablas de producción grandes de forma indefinida es muy costoso (y puede generar problemas de cumplimiento si hay PII presente).
-- MAGIC 
-- MAGIC Si desea purgar manualmente los archivos de datos antiguos, puede hacerlo con la operación **`VACUUM`**.
-- MAGIC 
-- MAGIC Descomente la siguiente celda y ejecútela con una retención de **`0 HORAS`** para mantener solo la versión actual:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC By default, **`VACUUM`** will prevent you from deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.  In our demos, you may see Databricks executing code that specifies a retention of **`0 HOURS`**. This is simply to demonstrate the feature and is not typically done in production.  
-- MAGIC 
-- MAGIC In the following cell, we:
-- MAGIC 1. Turn off a check to prevent premature deletion of data files
-- MAGIC 1. Make sure that logging of **`VACUUM`** commands is enabled
-- MAGIC 1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC By running **`VACUUM`** and deleting the 10 files above, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

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
