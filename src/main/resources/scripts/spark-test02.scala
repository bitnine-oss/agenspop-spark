/*
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

## start shell (default memory 1G)
cd $SPARK_HOME

./build/mvn \
    -Dhadoop.version=2.10.0 \
    -Pyarn,hive,hive-thriftserver \
    -Pscala-2.11 \
    -Pkubernetes \
    -DskipTests \
    clean package

./dev/make-distribution.sh --name hadoop2.10 --tgz -Pkubernetes -Phadoop-2.10 -Phive -Phive-thriftserver -Pyarn

## start shell (default memory 1G)
bin/spark-shell --master spark://minmac:7077 --executor-memory 4G

## show all conf
sc.getConf.toDebugString
=>
spark.driver.bindAddress=127.0.0.1
spark.driver.host=127.0.0.1
spark.driver.port=51269

spark.conf.get("spark.sql.catalogImplementation")
=>
hive

## skip this lines
val sc = new SparkContext()
val spark = SparkSession.builder().config(sc.getConf).getOrCreate

## Spark is not running in local mode, 
## therefore the checkpoint directory must not be on the local filesystem.
sc.setCheckpointDir("hdfs:///tmp/checkpoint")
*/

//////////////////////////////////////////
// ** ref 
// https://jaceklaskowski.gitbooks.io/mastering-spark-sql/demo/demo-connecting-spark-sql-to-hive-metastore.html

// check hive catalog
spark.conf.get("spark.sql.catalogImplementation")

// show tables in default database
spark.catalog.listTables.show

// show tables in externalCatalog (hive)
spark.sharedState.externalCatalog.listTables("default")

// test access hadoop using hive (limit 10, with all columns)
spark.table("pokes1").show(10, false)

// show tables in es_test
spark.sql("show tables in es_test").show()

// test access elastic-hdaoop using hive
spark.table("es_test.tbl_dogs").show

/////////////////////////////////////////
// global_temp_view to hive table

// registerTempTable is deprecation on spark 2
scala> dfV.registerTempTable("nodes")
warning: there was one deprecation warning; re-run with -deprecation for details

scala> spark.sql("select * from nodes").show
+-------------------+----------+--------+--------+--------------------+
|          timestamp|datasource|      id|   label|          properties|
+-------------------+----------+--------+--------+--------------------+
|2019-01-21 21:21:21|    modern|modern_1|  person|[[name, java.lang...|
|2019-11-11 21:11:21|    modern|modern_6|  person|[[name, java.lang...|
|2019-03-23 23:23:23|    modern|modern_2|  person|[[name, java.lang...|
|2019-05-25 15:25:25|    modern|modern_3|software|[[name, java.lang...|
|2019-09-29 21:21:21|    modern|modern_5|software|[[name, java.lang...|
|2019-07-27 21:21:21|    modern|modern_4|  person|[[name, java.lang...|
+-------------------+----------+--------+--------+--------------------+


// ** ref 
// https://stackoverflow.com/questions/42774187/spark-createorreplacetempview-vs-createglobaltempview

// dfV.createOrReplaceTempView("modern_v")      // - during spark session
//  ==> spark.catalog.dropTempView("tempViewName")

scala> dfV.createGlobalTempView("modern_v")     // - during spark application
// drop ==> spark.catalog.dropGlobalTempView("tempViewName")


scala> spark.sql("select * from global_temp.modern_v").show
+-------------------+----------+--------+--------+--------------------+
|          timestamp|datasource|      id|   label|          properties|
+-------------------+----------+--------+--------+--------------------+
|2019-01-21 21:21:21|    modern|modern_1|  person|[[name, java.lang...|
|2019-11-11 21:11:21|    modern|modern_6|  person|[[name, java.lang...|
|2019-03-23 23:23:23|    modern|modern_2|  person|[[name, java.lang...|
|2019-05-25 15:25:25|    modern|modern_3|software|[[name, java.lang...|
|2019-09-29 21:21:21|    modern|modern_5|software|[[name, java.lang...|
|2019-07-27 21:21:21|    modern|modern_4|  person|[[name, java.lang...|
+-------------------+----------+--------+--------+--------------------+

scala> spark.sql("desc modern_v").show
+----------+--------------------+-------+
|  col_name|           data_type|comment|
+----------+--------------------+-------+
| timestamp|              string|   null|
|datasource|              string|   null|
|        id|              string|   null|
|     label|              string|   null|
|properties|array<struct<key:...|   null|
+----------+--------------------+-------+

scala> sql("CREATE TABLE modern_v1 STORED AS PARQUET select * from modern_v")
res25: org.apache.spark.sql.DataFrame = []

scala> sql("CREATE TABLE es_test.modern_v STORED AS PARQUET select * from modern_v")
res26: org.apache.spark.sql.DataFrame = []

CREATE EXTERNAL TABLE tbl_dogs(
  breed STRING, 
  sex STRING
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.resource'='hive_test',
  'es.nodes'='192.168.0.30:39200',
  'es.net.http.auth.user'='elastic',
  'es.net.http.auth.pass'='bitnine',
  'es.index.auto.create'='true',
  'es.mapping.names'='breed:breed, sex:sex'
);

CREATE TABLE ht0_dogs (
  breed STRING, 
  sex STRING
) using hive;

CREATE TABLE demo_sales
(id BIGINT, qty BIGINT, name STRING)
COMMENT 'Demo: Connecting Spark SQL to Hive Metastore'
PARTITIONED BY (rx_mth_cd STRING COMMENT 'Prescription Date YYYYMM aggregated')
STORED AS PARQUET;


create table if not exists es_test.ht1_dogs
stored as parquet
as 
select * from es_test.tbl_dogs where 1=1;

create table if not exists es_test.ht2_dogs
as 
select * from es_test.tbl_dogs where 1=1;

create table if not exists es_test.ht3_dogs
using hive
as 
select * from es_test.tbl_dogs where 1=1;

mvn clean package -DskipTests -Phadoop-2 -Pdist

































