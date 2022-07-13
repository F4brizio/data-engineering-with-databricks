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
-- MAGIC 
-- MAGIC # Gestión de tablas delta
-- MAGIC 
-- MAGIC Si conoce algún tipo de SQL, ya tiene gran parte del conocimiento que necesitará para trabajar de manera efectiva en el lago de datos.
-- MAGIC 
-- MAGIC En este notebook, exploraremos la manipulación básica de datos y tablas con SQL en Databricks.
-- MAGIC 
-- MAGIC Tenga en cuenta que Delta Lake es el formato predeterminado para todas las tablas creadas con Databricks; si ha estado ejecutando instrucciones SQL en Databricks, es probable que ya esté trabajando con Delta Lake.
-- MAGIC 
-- MAGIC ## Objetivos de aprendizaje
-- MAGIC Al final de esta lección, debería ser capaz de:
-- MAGIC * Crear tablas de Delta Lake
-- MAGIC * Consulta de datos de las tablas de Delta Lake
-- MAGIC * Insertar, actualizar y eliminar registros en las tablas de Delta Lake
-- MAGIC * Escribir upsert statements con Delta Lake
-- MAGIC * Drop Delta Lake tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Creación de una tabla Delta
-- MAGIC 
-- MAGIC No necesita escribir mucho código para crear una tabla con Delta Lake. Hay varias formas de crear tablas de Delta Lake que veremos a lo largo del curso. Comenzaremos con uno de los métodos más fáciles: registrar una mesa de Delta Lake vacía.
-- MAGIC 
-- MAGIC Nosotros necesitamos:
-- MAGIC - Una instrucción **`CREATE TABLE`**
-- MAGIC - Un nombre de tabla (abajo usamos **`students`**)
-- MAGIC - Un esquema
-- MAGIC 
-- MAGIC **NOTE:** In Databricks Runtime 8.0 and above, Delta Lake is the default format and you don’t need **`USING DELTA`**.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Si tratamos de regresar y ejecutar esa celda nuevamente... ¡se producirá un error! Esto es de esperar, dado que la tabla ya existe, recibimos un error.
-- MAGIC 
-- MAGIC Podemos agregar un argumento adicional, **`IF NOT EXISTS`** que verifica si la tabla existe. Esto superará nuestro error.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Inserción de datos
-- MAGIC La mayoría de las veces, los datos se insertarán en las tablas como resultado de una consulta de otra fuente.
-- MAGIC 
-- MAGIC Sin embargo, al igual que en SQL estándar, también puede insertar valores directamente, como se muestra aquí.

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC En la celda de arriba, completamos tres instrucciones separadas **`INSERT`**. Cada uno de estos se procesa como una transacción separada con sus propias garantías ACID. Con mayor frecuencia, insertaremos muchos registros en una sola transacción.

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que Databricks no tiene una palabra clave **`COMMIT`**; las transacciones se ejecutan tan pronto como se ejecutan y se confirman cuando tienen éxito.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Querying a Delta Table
-- MAGIC 
-- MAGIC You probably won't be surprised that querying a Delta Lake table is as easy as using a standard **`SELECT`** statement.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Lo que puede sorprenderlo es que Delta Lake garantiza que cualquier lectura de una tabla **siempre** devolverá la versión más reciente de la tabla, y que nunca encontrará un estado de bloqueo debido a operaciones en curso.
-- MAGIC 
-- MAGIC Para repetir: las lecturas de tablas nunca pueden entrar en conflicto con otras operaciones, y la versión más reciente de sus datos está disponible de inmediato para todos los clientes que pueden consultar su lakehouse. Debido a que toda la información de transacciones se almacena en el almacenamiento de objetos en la nube junto con sus archivos de datos, las lecturas simultáneas en las tablas de Delta Lake están limitadas solo por los límites estrictos del almacenamiento de objetos en los proveedores de la nube. (**NOTA**: no es infinito, pero son al menos miles de lecturas por segundo).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Actualización de registros
-- MAGIC 
-- MAGIC La actualización de registros también brinda garantías atómicas: realizamos una lectura instantánea de la versión actual de nuestra tabla, buscamos todos los campos que coinciden con nuestra cláusula **`WHERE`** y luego aplicamos los cambios como se describe.
-- MAGIC 
-- MAGIC A continuación, encontramos a todos los estudiantes que tienen un nombre que comienza con la letra **T** y agregan 1 al número en su columna **`value`**.

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Consulte la tabla nuevamente para ver estos cambios aplicados.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Eliminación de registros
-- MAGIC 
-- MAGIC Las eliminaciones también son atómicas, por lo que no hay riesgo de tener éxito solo parcialmente al eliminar datos de su lago de datos.
-- MAGIC 
-- MAGIC Una declaración **`DELETE`** puede eliminar uno o varios registros, pero siempre dará como resultado una sola transacción.

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Usando Merge
-- MAGIC 
-- MAGIC Algunos sistemas SQL tienen el concepto de upsert, que permite ejecutar actualizaciones, inserciones y otras manipulaciones de datos como un solo comando.
-- MAGIC 
-- MAGIC Databricks usa la palabra clave **`MERGE`** para realizar esta operación.
-- MAGIC 
-- MAGIC Considere la siguiente vista temporal, que contiene 4 registros que podrían generarse mediante una fuente de captura de datos modificados (CDC).

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Usando la sintaxis que hemos visto hasta ahora, podríamos filtrar desde esta vista por tipo para escribir 3 declaraciones, una para insertar, actualizar y eliminar registros. Pero esto daría como resultado 3 transacciones separadas; si alguna de estas transacciones fallara, podría dejar nuestros datos en un estado no válido.
-- MAGIC 
-- MAGIC En cambio, combinamos estas acciones en una sola transacción atómica, aplicando los 3 tipos de cambios juntos.
-- MAGIC 
-- MAGIC Las declaraciones **`MERGE`** deben tener al menos un campo para hacer coincidir, y cada cláusula **`WHEN MATCHED`** o **`WHEN NOT MATCHED`** puede tener cualquier cantidad de declaraciones condicionales adicionales.
-- MAGIC 
-- MAGIC Aquí, hacemos coincidir nuestro campo **`id`** y luego filtramos en el campo **`tipo`** para actualizar, eliminar o insertar nuestros registros de manera adecuada.

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC Tenga en cuenta que solo 3 registros se vieron afectados por nuestra instrucción **`MERGE`**; uno de los registros en nuestra tabla de actualizaciones no tenía un **`id`** coincidente en la tabla de estudiantes, pero se marcó como una **`update`**. Según nuestra lógica personalizada, ignoramos este registro en lugar de insertarlo.
-- MAGIC 
-- MAGIC ¿Cómo modificaría la declaración anterior para incluir registros no coincidentes marcados como **`update`** en la cláusula final **`INSERT`**?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Dropping a Table
-- MAGIC 
-- MAGIC Suponiendo que tenga los permisos adecuados en la tabla de destino, puede eliminar permanentemente los datos en la casa del lago usando un comando **`DROP TABLE`**.
-- MAGIC 
-- MAGIC **NOTA**: Más adelante en el curso, analizaremos las listas de control de acceso a tablas (ACLs) y los permisos predeterminados. En una casa del lago configurada correctamente, los usuarios **no** deberían poder eliminar las tablas de producción.

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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
