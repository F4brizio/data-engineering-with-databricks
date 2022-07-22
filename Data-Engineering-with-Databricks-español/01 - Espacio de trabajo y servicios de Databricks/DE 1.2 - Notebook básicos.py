# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Conceptos básicos de Notebook
# MAGIC 
# MAGIC Los notebooks son el medio principal para desarrollar y ejecutar código de forma interactiva en Databricks. Esta lección proporciona una introducción básica al trabajo con cuadernos de Databricks.
# MAGIC 
# MAGIC Si ha usado cuadernos de Databricks anteriormente, pero esta es la primera vez que ejecuta un cuaderno en Databricks Repos, notará que la funcionalidad básica es la misma. En la siguiente lección, revisaremos algunas de las funciones que Databricks Repos agrega a los cuadernos.
# MAGIC 
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al final de esta lección, debería ser capaz de:
# MAGIC * Adjunte un notebook a un clúster
# MAGIC * Ejecutar una celda en un notebook
# MAGIC * Establecer el idioma para un notebook
# MAGIC * Describir y usar comandos mágicos
# MAGIC * Crear y ejecutar una celda SQL
# MAGIC * Crear y ejecutar una celda de Python
# MAGIC * Crear una celda de descuento
# MAGIC * Exportar un notebook de Databricks
# MAGIC * Exportar una colección de notebooks de Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Adjuntar a un clúster
# MAGIC 
# MAGIC En la lección anterior, debería haber implementado un clúster o identificado un clúster que un administrador configuró para que lo use.
# MAGIC 
# MAGIC Directamente debajo del nombre de este notebook en la parte superior de la pantalla, use la lista desplegable para conectar este notebook a su clúster.
# MAGIC 
# MAGIC **NOTA**: La implementación de un clúster puede tardar varios minutos. Aparecerá una flecha verde a la derecha del nombre del clúster una vez que se hayan implementado los recursos. Si su clúster tiene un círculo gris sólido a la izquierda, deberá seguir las instrucciones para <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">iniciar un clúster</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Notebooks Basics
# MAGIC 
# MAGIC Notebooks provide cell-by-cell execution of code. Multiple languages can be mixed in a notebook. Users can add plots, images, and markdown text to enhance their code.
# MAGIC 
# MAGIC Throughout this course, our notebooks are designed as learning instruments. Notebooks can be easily deployed as production code with Databricks, as well as providing a robust toolset for data exploration, reporting, and dashboarding.
# MAGIC 
# MAGIC ### Running a Cell
# MAGIC * Run the cell below using one of the following options:
# MAGIC   * **CTRL+ENTER** or **CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
# MAGIC   * Using **Run Cell**, **Run All Above** or **Run All Below** as seen here<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC **NOTE**: Cell-by-cell code execution means that cells can be executed multiple times or out of order. Unless explicitly instructed, you should always assume that the notebooks in this course are intended to be run one cell at a time from top to bottom. If you encounter an error, make sure you read the text before and after a cell to ensure that the error wasn't an intentional learning moment before you try to troubleshoot. Most errors can be resolved by either running earlier cells in a notebook that were missed or re-executing the entire notebook from the top.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Setting the Default Notebook Language
# MAGIC 
# MAGIC The cell above executes a Python command, because our current default language for the notebook is set to Python.
# MAGIC 
# MAGIC Databricks notebooks support Python, SQL, Scala, and R. A language can be selected when a notebook is created, but this can be changed at any time.
# MAGIC 
# MAGIC The default language appears directly to the right of the notebook title at the top of the page. Throughout this course, we'll use a blend of SQL and Python notebooks.
# MAGIC 
# MAGIC We'll change the default language for this notebook to SQL.
# MAGIC 
# MAGIC Steps:
# MAGIC * Click on the **Python** next to the notebook title at the top of the screen
# MAGIC * In the UI that pops up, select **SQL** from the drop down list 
# MAGIC 
# MAGIC **NOTE**: In the cell just before this one, you should see a new line appear with <strong><code>&#37;python</code></strong>. We'll discuss this in a moment.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Create and Run a SQL Cell
# MAGIC 
# MAGIC * Highlight this cell and press the **B** button on the keyboard to create a new cell below
# MAGIC * Copy the following code into the cell below and then run the cell
# MAGIC 
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC 
# MAGIC **NOTE**: There are a number of different methods for adding, moving, and deleting cells including GUI options and keyboard shortcuts. Refer to the <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">docs</a> for details.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Magic Commands
# MAGIC * Magic commands are specific to the Databricks notebooks
# MAGIC * They are very similar to magic commands found in comparable notebook products
# MAGIC * These are built-in commands that provide the same outcome regardless of the notebook's language
# MAGIC * A single percent (%) symbol at the start of a cell identifies a magic command
# MAGIC   * You can only have one magic command per cell
# MAGIC   * A magic command must be the first thing in a cell

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Language Magics
# MAGIC Language magic commands allow for the execution of code in languages other than the notebook's default. In this course, we'll see the following language magics:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC 
# MAGIC Adding the language magic for the currently set notebook type is not necessary.
# MAGIC 
# MAGIC When we changed the notebook language from Python to SQL above, existing cells written in Python had the <strong><code>&#37;python</code></strong> command added.
# MAGIC 
# MAGIC **NOTE**: Rather than changing the default language of a notebook constantly, you should stick with a primary language as the default and only use language magics as necessary to execute code in another language.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Markdown
# MAGIC 
# MAGIC The magic command **&percnt;md** allows us to render Markdown in a cell:
# MAGIC * Double click this cell to begin editing it
# MAGIC * Then hit **`Esc`** to stop editing
# MAGIC 
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC 
# MAGIC This is a test of the emergency broadcast system. This is only a test.
# MAGIC 
# MAGIC This is text with a **bold** word in it.
# MAGIC 
# MAGIC This is text with an *italicized* word in it.
# MAGIC 
# MAGIC This is an ordered list
# MAGIC 0. once
# MAGIC 0. two
# MAGIC 0. three
# MAGIC 
# MAGIC This is an unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC 
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC 
# MAGIC Images:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC 
# MAGIC And of course, tables:
# MAGIC 
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### %run
# MAGIC * You can run a notebook from another notebook by using the magic command **%run**
# MAGIC * Notebooks to be run are specified with relative paths
# MAGIC * The referenced notebook executes as if it were part of the current notebook, so temporary views and other local declarations will be available from the calling notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Uncommenting and executing the following cell will generate the following error:<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC But we can declare it and a handful of other variables and functions buy running this cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC El notebook **`../Includes/Classroom-Setup-1.2`** al que hicimos referencia incluye lógica para crear y **`USE`** una base de datos, además de crear la vista temporal **`demo_temp_vw`**.
# MAGIC 
# MAGIC Podemos ver que esta vista temporal ahora está disponible en nuestra sesión de notebook actual con la siguiente consulta.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Usaremos este patrón de notebooks de "configuración" a lo largo del curso para ayudar a configurar el entorno para lecciones y laboratorios.
# MAGIC 
# MAGIC Estas variables, funciones y otros objetos "proporcionados" deben ser fácilmente identificables porque forman parte del objeto **`DA`**, que es una instancia de **`DBAcademyHelper`**.
# MAGIC 
# MAGIC Con eso en mente, la mayoría de las lecciones usarán variables derivadas de su nombre de usuario para organizar archivos y bases de datos.
# MAGIC 
# MAGIC Este patrón nos permite evitar la colisión con otros usuarios en un espacio de trabajo compartido.
# MAGIC 
# MAGIC La siguiente celda usa Python para imprimir algunas de esas variables previamente definidas en el script de configuración de este cuaderno:

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.db_name:           {DA.db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Además de esto, estas mismas variables se "inyectan" en el contexto de SQL para que podamos usarlas en las declaraciones de SQL.
# MAGIC 
# MAGIC Hablaremos más sobre esto más adelante, pero puede ver un ejemplo rápido en la siguiente celda.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Tenga en cuenta la diferencia sutil pero importante en el uso de mayúsculas y minúsculas entre las palabras **`da`** y **`DA`* * en estos dos ejemplos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.db_name}' as database_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Utilidades de Databricks
# MAGIC Los cuadernos de Databricks proporcionan una serie de comandos de utilidad para configurar e interactuar con el entorno: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC 
# MAGIC A lo largo de este curso, ocasionalmente usaremos **`dbutils.fs.ls()`** para enumerar directorios de archivos de las celdas de Python.

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## display()
# MAGIC 
# MAGIC Al ejecutar consultas SQL desde celdas, los resultados siempre se mostrarán en un formato tabular representado.
# MAGIC 
# MAGIC Cuando tenemos datos tabulares devueltos por una celda de Python, podemos llamar a **`display`** para obtener el mismo tipo de vista previa.
# MAGIC 
# MAGIC Aquí, envolveremos el comando de lista anterior en nuestro sistema de archivos con **`display`**.

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))
#display(dbutils.fs.ls("/"))
#display(dbutils.fs.ls("/databricks-datasets/COVID/covid-19-data"))
#display(dbutils.fs.ls("/databricks-datasets/COVID/covid-19-data/us.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC El comando **`display()`** tiene las siguientes capacidades y limitaciones:
# MAGIC * Vista previa de resultados limitada a 1000 registros
# MAGIC * Proporciona un botón para descargar datos de resultados como CSV
# MAGIC * Permite renderizar parcelas/plots

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Descarga de Notebooks
# MAGIC 
# MAGIC Hay varias opciones para descargar Notebooks individuales o colecciones de Notebooks.
# MAGIC 
# MAGIC Aquí, recorrerá el proceso para descargar este Notebook, así como una colección de todos los cuadernos de este curso.
# MAGIC 
# MAGIC ### Descargar un Notebook
# MAGIC 
# MAGIC Pasos:
# MAGIC * Haga clic en la opción **Archivo** a la derecha de la selección de grupos en la parte superior de la libreta
# MAGIC * En el menú que aparece, coloque el cursor sobre **Exportar** y luego seleccione **Archivo de origen**
# MAGIC 
# MAGIC El Notebook se descargará en su computadora portátil personal. Se nombrará con el nombre del Notebook actual y tendrá la extensión de archivo para el idioma predeterminado. Puede abrir este cuaderno con cualquier editor de archivos y ver el contenido sin procesar de los cuadernos de Databricks.
# MAGIC 
# MAGIC Estos archivos de origen se pueden cargar en cualquier área de trabajo de Databricks.
# MAGIC 
# MAGIC ### Descargar una colección de Notebooks
# MAGIC 
# MAGIC **NOTA**: Las siguientes instrucciones asumen que ha importado estos materiales usando **Repos**.
# MAGIC 
# MAGIC Pasos:
# MAGIC * Haga clic en ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** en la barra lateral izquierda
# MAGIC   * Esto debería darle una vista previa de los directorios principales para este Notebook
# MAGIC * En el lado izquierdo de la vista previa del directorio, alrededor del centro de la pantalla, debe haber una flecha hacia la izquierda. Haga clic aquí para ascender en la jerarquía de archivos.
# MAGIC * Debería ver un directorio llamado **Ingeniería de datos con Databricks**. Haga clic en la flecha hacia abajo/cheurón para abrir un menú
# MAGIC * En el menú, pase el cursor sobre **Exportar** y seleccione **Archivo DBC**
# MAGIC 
# MAGIC El archivo DBC (Databricks Cloud) que se descarga contiene una colección comprimida de los directorios y Notebooks de este curso. Los usuarios no deben intentar editar estos archivos DBC localmente, pero se pueden cargar de forma segura en cualquier espacio de trabajo de Databricks para mover o compartir contenidos de blocs de notas.
# MAGIC 
# MAGIC **NOTA**: Al descargar una colección de DBC, también se exportarán las vistas previas de los resultados y los gráficos. Al descargar cuadernos de origen, solo se guardará el código.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Learning More
# MAGIC 
# MAGIC We like to encourage you to explore the documentation to learn more about the various features of the Databricks platform and notebooks.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importing notebooks - Supported Formats</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Administration Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Cluster Configuration</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Release Notes</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## One more note! 
# MAGIC 
# MAGIC At the end of each lesson you will see the following command, **`DA.cleanup()`**.
# MAGIC 
# MAGIC This method drops lesson-specific databases and working directories in an attempt to keep your workspace clean and maintain the immutability of each lesson.
# MAGIC 
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
