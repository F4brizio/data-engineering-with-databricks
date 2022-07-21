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
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Primeros pasos con la plataforma de Databricks
# MAGIC 
# MAGIC Este notebook proporciona una revisión práctica de algunas de las funciones básicas del espacio de trabajo de ingeniería y ciencia de datos de Databricks.
# MAGIC 
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al final de este laboratorio, usted debería ser capaz de:
# MAGIC - Cambiar el nombre de un notebook y cambiar el idioma predeterminado
# MAGIC - Adjuntar un clúster
# MAGIC - Usa el comando mágico **`%run`**
# MAGIC - Ejecutar celdas de Python y SQL
# MAGIC - Crear una celda Markdown

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Cambiar el nombre de un Notebook
# MAGIC 
# MAGIC Cambiar el nombre de un Notebook es fácil. Haga clic en el nombre en la parte superior de esta página, luego realice cambios en el nombre. Para facilitar la navegación de regreso a este bloc de notas más adelante en caso de que lo necesite, agregue una breve cadena de prueba al final del nombre existente.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Adjuntar un clúster
# MAGIC 
# MAGIC La ejecución de celdas en un notebook requiere recursos informáticos, que son proporcionados por clústeres. La primera vez que ejecute una celda en un bloc de notas, se le pedirá que se conecte a un clúster si aún no se ha conectado uno.
# MAGIC 
# MAGIC Adjunte un clúster a este cuaderno ahora haciendo clic en el menú desplegable cerca de la esquina superior izquierda de esta página. Seleccione el clúster que creó anteriormente. Esto borrará el estado de ejecución del notebook y conectará el notebook al clúster seleccionado.
# MAGIC 
# MAGIC Tenga en cuenta que el menú desplegable ofrece la opción de iniciar o reiniciar el clúster según sea necesario. También puede desconectar y volver a conectar a un grupo en un solo movimiento. Esto es útil para borrar el estado de ejecución cuando sea necesario.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Usando %run
# MAGIC 
# MAGIC Los proyectos complejos de cualquier tipo pueden beneficiarse de la capacidad de dividirlos en componentes más simples y reutilizables.
# MAGIC 
# MAGIC En el contexto de los cuadernos de Databricks, esta función se proporciona a través del comando mágico **`%run`**.
# MAGIC 
# MAGIC Cuando se usa de esta manera, las variables, funciones y bloques de código se convierten en parte del contexto de programación actual.
# MAGIC 
# MAGIC Considere este ejemplo:
# MAGIC 
# MAGIC **`Notebook_A`** tiene cuatro comandos:
# MAGIC   1. **`name = "Juan"`**
# MAGIC   2. **`print(f"Hola {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Bienvenido de nuevo {full_name}`**
# MAGIC 
# MAGIC **`Notebook_B`** solo tiene un comando:
# MAGIC   1. **`full_name = f"{nombre} Doe"`**
# MAGIC 
# MAGIC Si ejecutamos **`Notebook_B`** no se ejecutará porque la variable **`name`** no está definida en **`Notebook_B`**
# MAGIC 
# MAGIC Del mismo modo, uno podría pensar que **`Notebook_A`** fallaría porque usa la variable **`full_name`** que tampoco está definida en **`Notebook_A`**, ¡pero no es así!
# MAGIC 
# MAGIC Lo que realmente sucede es que los dos notebooks se fusionan como vemos a continuación y **luego** se ejecutan:
# MAGIC 1. **`name = "Juan"`**
# MAGIC 2. **`print(f"Hola {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Bienvenido de nuevo {full_name}`**
# MAGIC 
# MAGIC Y proporcionando así el comportamiento esperado:
# MAGIC * **`Hola Juan`**
# MAGIC * **`Bienvenido de nuevo John Doe`**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC La carpeta que contiene este notebook contiene una subcarpeta llamada **`ExampleSetupFolder`**, que a su vez contiene un cuaderno llamado **`example-setup`**.
# MAGIC 
# MAGIC Este cuaderno simple declara la variable **`my_name`**, la establece en **`Ninguno`** y luego crea un DataFrame llamado **`example_df`**.
# MAGIC 
# MAGIC Abra el cuaderno de configuración de ejemplo y modifíquelo para que el nombre no sea **`Ninguno`** sino su nombre (o el nombre de cualquier persona) entre comillas, y para que las siguientes dos celdas se ejecuten sin arrojar un **`AssertionError`**.

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Run a Python cell
# MAGIC 
# MAGIC Run the following cell to verify that the **`example-setup`** notebook was executed by displaying the **`example_df`** Dataframe. This table consists of 16 rows of increasing values.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Detach and Reattach a Cluster
# MAGIC 
# MAGIC While attaching to clusters is a fairly common task, sometimes it is useful to detach and re-attach in one single operation. The main side-effect this achieves is clearing the execution state. This can be useful when you want to test cells in isolation, or you simply want to reset the execution state.
# MAGIC 
# MAGIC Revisit the cluster dropdown. In the menu item representing the currently attached cluster, select the **Detach & Re-attach** link.
# MAGIC 
# MAGIC Notice that the output from the cell above remains since results and execution state are unrelated, but the execution state is cleared. This can be verified by attempting to re-run the cell above. This fails, since the **`example_df`** variable has been cleared, along with the rest of the state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Change Language
# MAGIC 
# MAGIC Notice that the default language for this notebook is set to Python. Change this by clicking the **Python** button to the right of the notebook name. Change the default language to SQL.
# MAGIC 
# MAGIC Notice that the Python cells are automatically prepended with a <strong><code>&#37;python</code></strong> magic command to maintain validity of those cells. Notice that this operation also clears the execution state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Create a Markdown Cell
# MAGIC 
# MAGIC Add a new cell below this one. Populate with some Markdown that includes at least the following elements:
# MAGIC * A header
# MAGIC * Bullet points
# MAGIC * A link (using your choice of HTML or Markdown conventions)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Run a SQL cell
# MAGIC 
# MAGIC Run the following cell to query a Delta table using SQL. This executes a simple query against a table is backed by a Databricks-provided example dataset included in all DBFS installations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Execute the following cell to view the underlying files backing this table.

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Review Changes
# MAGIC 
# MAGIC Assuming you have imported this material into your workspace using a Databricks Repo, open the Repo dialog by clicking the **`published`** branch button at the top-left corner of this page. You should see three changes:
# MAGIC 1. **Removed** with the old notebook name
# MAGIC 1. **Added** with the new notebook name
# MAGIC 1. **Modified** for creating a markdown cell above
# MAGIC 
# MAGIC Use the dialog to revert the changes and restore this notebook to its original state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC By completing this lab, you should now feel comfortable manipulating notebooks, creating new cells, and running notebooks within notebooks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
