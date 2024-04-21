<h1>Práctica Integradora BIG DATA</h1>

<img src="imagenes/img1.jpg" width=600px align="center">

<p align="justify">En esta práctica integradora el objetivo es simular la situación que se detalla a continuación.</p>

<p align="justify"><em>Desde un área de innovación solicitan construir un MVP(Producto viable mínimo) de un ambiente de Big Data donde se deban cargar unos archivos CSV que anteriormente se utilizaban en un datawarehouse en MySQl, pero ahora en un entorno de Hadoop.</em></p>

<p align="justify"><em>Desde la gerencia de Infraestructura no están muy convencidos de utilizar esta tecnología por lo que no se asigno presupuesto alguna para esta iniciativa, de forma tal que por el momento no es posible utilizar un Vendor(Azure, AWS, Google) para implementar dicho entorno, es por esto que todo el MVP se deberá implementar utilizando Docker de forma tal que se pueda hacer una demo al sector de infraestructura mostrando las ventajas de utilizar tecnologías de Big Data.</em></p>

 <h2>Entorno Docker con Hadoop, Spark y Hive</h2>

Se pesenta un entorno Docker con Hadoop (HDFS) y la implementación de:
<ul>
  <li>Spark</li>
  <li>Hive</li>
  <li>Hbase</li>
  <li>MongoDB</li>
  <li>Neo4J</li>
  <li>Zeppelin</li>
  <li>Kafka</li>
</ul>


Es importante mencionar que el entorno completo consume muchos recursos de su equipo, motivo por el cuál, se propondrán ejercicios pero con ambientes reducidos, en función de las herramientas utilizadas.

Ejecute `docker network inspect` en la red (por ejemplo, `docker-hadoop-spark-hive_default`) para encontrar la IP en la que se publican las interfaces de hadoop. Acceda a estas interfaces con las siguientes URL:

```
Namenode: http://<IP_Anfitrion>:9870/dfshealth.html#tab-overview
Datanode: http://<IP_Anfitrion>:9864/
Spark master: http://<IP_Anfitrion>:8080/
Spark worker: http://<IP_Anfitrion>:8081/	
HBase Master-Status: http://<IP_Anfitrion>:16010
HBase Zookeeper_Dump: http://<IP_Anfitrion>:16010/zk.jsp
HBase Region_Server: http://<IP_Anfitrion>:16030
Zeppelin: http://<IP_Anfitrion>:8888
Neo4j: http://<IP_Anfitrion>:7474
```

Para implementar ejecute
```
  git clone https://github.com/lopezdar222/herramientas_big_data
  cd herramientas_big_data
  sudo docker-compose -f docker-compose-vX.yml up -d
```

<h1>1) HFDS</h1>
Se puede utilizar el entorno docker-compose-v1.yml de la siguiente manera:

```
  sudo docker-compose -f docker-compose-v1.yml up -d
```
Explicar el flag -f

Explicar el flag -d

Copiar los archivos ubicados en la carpeta Datasets, dentro del contenedor "namenode"

```
  sudo docker exec -it namenode bash
  cd home
  mkdir Datasets
  exit
  sudo docker cp <path><archivo> namenode:/home/Datasets/<archivo>
```
Si bien en el codigo anterior se mostro una manera general de realizar el proceso de copiar archivos de la carpeta Datasets de los archivos descargados al contenedor "namemode" a continuación se muestra un script, Paso00.sh, donde esto se realiza:

```
 sudo docker cp Datasets/canaldeventa/CanalDeVenta.csv namenode:/home/Datasets/canaldeventa/CanalDeVenta.csv
 sudo docker cp Datasets/calendario/Calendario.csv namenode:/home/Datasets/calendario/Calendario.csv
 sudo docker cp Datasets/cliente/Cliente.csv namenode:/home/Datasets/cliente/Cliente.csv
 sudo docker cp Datasets/compra/Compra.csv namenode:/home/Datasets/compra/Compra.csv
 sudo docker cp Datasets/empleado/Empleado.csv namenode:/home/Datasets/empleado/Empleado.csv
 sudo docker cp Datasets/gasto/Gasto.csv namenode:/home/Datasets/gasto/Gasto.csv
 sudo docker cp Datasets/producto/Producto.csv namenode:/home/Datasets/producto/Producto.csv
 sudo docker cp Datasets/proveedor/Proveedor.csv namenode:/home/Datasets/proveedor/Proveedor.csv
 sudo docker cp Datasets/sucursal/Sucursal.csv namenode:/home/Datasets/sucursal/Sucursal.csv
 sudo docker cp Datasets/tipodegasto/TiposDeGasto.csv namenode:/home/Datasets/tipodegasto/TiposDeGasto.csv
 sudo docker cp Datasets/venta/Venta.csv namenode:/home/Datasets/venta/Venta.csv
 sudo docker cp Datasets/data_nvo/Cliente.csv namenode:/home/Datasets/data_nvo/Cliente.csv
 sudo docker cp Datasets/data_nvo/Empleado.csv namenode:/home/Datasets/data_nvo/Empleado.csv
 sudo docker cp Datasets/data_nvo/Producto.csv namenode:/home/Datasets/data_nvo/Producto.csv
 ```
Para ejecutar el script es tan simple como:

```
 ./Paso00.sh
```
Siempre y cuando estemos en la carpeta donde está el archivo .sh.

Ubicarse en el contenedor "namenode"
```
  sudo docker exec -it namenode bash
```
Crear un directorio en HDFS llamado "/data".
```
  hdfs dfs -mkdir -p /data
```
Copiar los archivos csv provistos a HDFS:
```
  hdfs dfs -put /home/Datasets/* /data
```
Este proceso de creación de la carpeta data y copiado de los arhivos, se puede ejercutar desde el scrip Paso01.sh que se muestra a continuación:

```
 sudo doccker exec -it namenode bash
 hdfs dfs -rm -R /data
 hdfs dfs -mkdir /data
 hdfs dfs -put /home/Datasets/* /data
```

Nota: Busque dfs.blocksize y dfs.replication en http://<IP_Anfitrion>:9870/conf para encontrar los valores de tamaño de bloque y factor de réplica respectivamente entre otras configuraciones del sistema Hadoop.

<h1>2) Hive</h1>

Se puede utilizar el entorno docker-compose-v2.yml mediante de la siguiente manera:

```
 sudo docker-compose -f docker-compose-v2.yml up -d
```

Crear tablas en Hive, a partir de los csv ingestados en HDFS.

Para crear tablas a partir de los CSV ingestatadose, se debe ubicar dentro del contenedor correspondiente al servidor de Hive ee ingresar a la linea de comandos mediante las siguieentes instrucciones:
```
  sudo docker exec -it hive-server bash
  hive
```
Pero tambien se pueden ejecutar scripts dentro del contenedor sin necesidad de ingresar a Hive. Por ejemplo para crear las tablas utilizaremos la siguiente linea:

```
hive -f script
```
Como se puede ver en la siguiente imagen nos muestra que no hubo errores y los tiempos ejecución.

Se verifica que se hayan creado las tablas mediante el uso de la linea de comando de Hive con el los siguientes comandos:

```
USE integrador;
SHOW tables;
```

Ahora tambien se verifica que las tablas no esten vacías mediante el comando:

```
SELECT COUNT(*) FROM compra
```
Se muestra una sola imagen para simplificar.


Este proceso de creación las tablas debe poder ejecutarse desde un shell script.

Nota: Para ejecutar un script de Hive, requiere el comando:

```
  hive -f <script.hql>
```
<h1>3) Formatos de Almacenamiento</h1>
En este caso se vuelven a crear las tablas del punto 2 pero en formato Parquet y comprimidas con Snappy para que sea más eficiente el proceso. Todo esto mediante la ejecución del siguiente script:

```
hive -f Paso03.hql
```
Ejecutado dentro del contenedor de Hive.
En la siguiente imagen podemos ver que el proceso se ejecuto correctamente e incluso vemos las primeras 10 filas de la tabla compra.


Además se agregaron particiones a la tabla gasto en base al IdTipoGasto como se puede ver en la descripcion de la tabla como se puede apreciar en la siguiente imagen.

<h1>4) SQL</h1>
Ahora para hacer más eficientes las consultas se crean indices en algunas tablas a modo de ejemplo para comparar los resultados.

En la siguiente imagen se crea un indice desde la consola para mostrar la sintaxis y los tiempos de ejecucion.

Debemos notar como disminuye el tiempo siendo una consulta realmente simple por lo que a medida que se incremente el volumen almacenado y se complejisen las consultas esto traera un benefifio en la eficiencia de la ejecución de las mismas.

<h1>5) NoSQL</h1>
En esta seccion trataremos base de datos No solo SQL que son mas flexibles que las SQL y permiten todo tipos de datos para almacenar.

Para esto usaremos el entorno 

```
sudo docker-compose -f docker-compose-v3.yml up -d
```
<h3>1) HBase</h3>

<h3>2) MongoDB</h3>

<h3>3) Neo4j</h3>

<h3>4) Zeppelin</h3>

