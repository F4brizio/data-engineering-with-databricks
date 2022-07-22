# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC # Crear y administrar Interactive Clusters
# MAGIC 
# MAGIC Un clúster de Databricks es un conjunto de configuraciones y recursos informáticos en los que ejecuta cargas de trabajo de ingeniería de datos, ciencia de datos y análisis de datos, como canalizaciones de ETL de producción, análisis de transmisión, análisis ad-hoc y aprendizaje automático. Estas cargas de trabajo se ejecutan como un conjunto de comandos en un cuaderno o como un trabajo automatizado.
# MAGIC 
# MAGIC Databricks hace una distinción entre clústeres de uso múltiple y clústeres de trabajo.
# MAGIC - Utiliza clústeres multipropósito para analizar datos de forma colaborativa mediante cuadernos interactivos.
# MAGIC - Utiliza grupos de trabajo para ejecutar trabajos automatizados rápidos y sólidos.
# MAGIC 
# MAGIC Esta demostración cubrirá la creación y administración de clústeres de Databricks multipropósito mediante el espacio de trabajo de ingeniería y ciencia de datos de Databricks.
# MAGIC 
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al final de esta lección, debería ser capaz de:
# MAGIC * Use la interfaz de usuario de clústeres para configurar e implementar un clúster
# MAGIC * Editar, finalizar, reiniciar y eliminar clústeres

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ## Crear clúster
# MAGIC 
# MAGIC Según el espacio de trabajo en el que esté trabajando actualmente, es posible que tenga o no privilegios de creación de clústeres.
# MAGIC 
# MAGIC Las instrucciones de esta sección asumen que **tiene** privilegios de creación de clústeres y que necesita implementar un nuevo clúster para ejecutar las lecciones de este curso.
# MAGIC 
# MAGIC **NOTA**: Consulte con su instructor o un administrador de la plataforma para confirmar si debe crear o no un nuevo clúster o conectarse a un clúster que ya se ha implementado. Las políticas de clúster pueden afectar sus opciones para la configuración del clúster.
# MAGIC 
# MAGIC Pasos:
# MAGIC 1. Utilice la barra lateral izquierda para navegar a la página **Calcular** haciendo clic en el ícono ![computar](https://files.training.databricks.com/images/clusters-icon.png)
# MAGIC 1. Haga clic en el botón azul **Crear clúster**
# MAGIC 1. Para el **Nombre del grupo**, use su nombre para que pueda encontrarlo fácilmente y el instructor pueda identificarlo fácilmente si tiene problemas
# MAGIC 1. Establezca el **Modo de clúster** en **Nodo único** (este modo es necesario para ejecutar este curso)
# MAGIC 1. Use la **versión de tiempo de ejecución de Databricks** recomendada para este curso
# MAGIC 1. Deje las casillas marcadas para la configuración predeterminada en **Opciones de piloto automático**
# MAGIC 1. Haga clic en el botón azul **Crear clúster**
# MAGIC 
# MAGIC **NOTA:** Los clústeres pueden tardar varios minutos en implementarse. Una vez que haya terminado de implementar un clúster, siéntase libre de continuar explorando la interfaz de usuario de creación de clúster.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> Clúster de un solo nodo requerido para este curso
# MAGIC **IMPORTANTE:** Este curso requiere que ejecute cuadernos en un clúster de un solo nodo.
# MAGIC 
# MAGIC Siga las instrucciones anteriores para crear un clúster que tenga **Modo de clúster** establecido en **`Nodo único`**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Administrar clústeres
# MAGIC 
# MAGIC Una vez que se crea el clúster, vuelva a la página **Calcular** para ver el clúster.
# MAGIC 
# MAGIC Seleccione un clúster para revisar la configuración actual.
# MAGIC 
# MAGIC Haga clic en el botón **Editar**. Tenga en cuenta que la mayoría de las configuraciones se pueden modificar (si tiene suficientes permisos). Cambiar la mayoría de las configuraciones requerirá que se reinicien los clústeres en ejecución.
# MAGIC 
# MAGIC **NOTA**: Usaremos nuestro clúster en la siguiente lección. Reiniciar, finalizar o eliminar su clúster puede retrasarlo mientras espera que se implementen nuevos recursos.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Reiniciar, Terminar y Eliminar
# MAGIC 
# MAGIC Tenga en cuenta que, si bien **Reiniciar**, **Terminar** y **Eliminar** tienen efectos diferentes, todos comienzan con un evento de terminación de clúster. (Los clústeres también terminarán automáticamente debido a la inactividad, suponiendo que se utilice esta configuración).
# MAGIC 
# MAGIC Cuando finaliza un clúster, se eliminan todos los recursos de la nube actualmente en uso. Esto significa:
# MAGIC * Se eliminarán las máquinas virtuales asociadas y la memoria operativa
# MAGIC * El almacenamiento de volumen adjunto se eliminará
# MAGIC * Se eliminarán las conexiones de red entre nodos
# MAGIC 
# MAGIC En resumen, todos los recursos previamente asociados con el entorno informático se eliminarán por completo. Esto significa que **cualquier resultado que deba conservarse debe guardarse en una ubicación permanente**. Tenga en cuenta que no perderá su código ni los archivos de datos que haya guardado correctamente.
# MAGIC 
# MAGIC El botón **Reiniciar** nos permitirá reiniciar manualmente nuestro clúster. Esto puede ser útil si necesitamos borrar completamente el caché en el clúster o si deseamos restablecer completamente nuestro entorno informático.
# MAGIC 
# MAGIC El botón **Terminar** nos permite detener nuestro clúster. Mantenemos nuestra configuración de clúster y podemos usar el botón **Reiniciar** para implementar un nuevo conjunto de recursos en la nube usando la misma configuración.
# MAGIC 
# MAGIC El botón **Eliminar** detendrá nuestro clúster y eliminará la configuración del clúster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
