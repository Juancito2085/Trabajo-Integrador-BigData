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
Este proceso de creación de la carpeta data y copiado de los arhivos, debe poder ejecutarse desde un shell script.

Nota: Busque dfs.blocksize y dfs.replication en http://<IP_Anfitrion>:9870/conf para encontrar los valores de tamaño de bloque y factor de réplica respectivamente entre otras configuraciones del sistema Hadoop.
