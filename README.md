# spark-examples
Spark examples using PySpark

## pokemon.py

1- Instalar HIVE:
	Seguir los pasos descritos en el siguiente enlance:
	http://saurzcode.in/2015/01/configure-mysql-metastore-hive/

	La instalación de HIVE en OSX la realicé mediante brew (brew install hive)

2- Si al ejecutar HIVE da error porque no puede acceder al directorio /user/hive/warehouse, deberemos crearlo

3- Debemos mover el driver JDBC a la carpeta $SPARK_DIR/jars
	cp /usr/local/Cellar/hive/2.1.0/libexec/lib/mysql-connector-java-5.1.40-bin.jar /Users/adrian/spark-2.0.2-bin-hadoop2.7/jars/

	En el ejemplo de arriba ya había copiado previamente el .jar al directorio donde he instalado HIVE (si no lo tuviésemos habría que descargar el driver JDBC)

4- Copiar el fichero hive-site.xml al directorio $SPARK_HOME/conf
	cp /usr/local/Cellar/hive/2.1.0/libexec/conf/hive-site.xml /Users/adrian/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml

	Asegurarnos de que hive-site.xml contiene las siguientes líneas:
	<property>
      <name>hive.metastore.warehouse.dir</name>
      <value>/Users/adrian/Documents/hive-warehouse</value>
    </property>

    Por alguna razón aún cambiando por código la ruta del Hive Warehouse no me cogía la ruta:
    hive_context.setConf("hive.warehouse.dir", "/Users/adrian/Documents/hive-warehouse") # Esto no funciona

5- Al ejecutar el script creará los ficheros con la tabla HIVE en /user/hive/warehouse/normal_pokemons.
Como vemos podemos acceder a la tabla tanto desde HIVE como desde Spark

6- Además, si vamos a MySQL y consultamos el metastore de HIVE, podemos ver lo siguiente:
mysql> use metastore;
Vemos que la tabla se ha registrado correctamente y que todo está OK
