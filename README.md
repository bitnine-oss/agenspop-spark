# AgensPop-Spark
AgensPop-Spark is a library that facilitates the integration of AgensGraph, a graph database, with Apache Spark, a distributed computing framework. This integration enables users to perform large-scale graph analytics by leveraging the power of both systems.

### Summary

An analytical backend server that can perform graph analysis with Spark using es-hadoop.

- build : `mvn clean install -DskipTests`
- deploy : `target/agenspop-spark-0.7.3.jar`
- run : `java -jar <jar_file> --spring.config.name=es-config`
- demo page : `http://<IP:8081>/index.html`

### Preparations

1) Install `spark-2.4.6-bin-hadoop2.7`

- Create `log4j.properties` (copy template)
- The installation location is used as `spark-home`

2) Write `es-config.yml`

- In `agens.elasticsearch`: `host`, `port`, `vertex-index`, `edge-index` Settings
- In `agens.spark`: `app-name` and `spark-home` Settings
- **<caution>** agens.spark.master-uri is fixed to `'local'` (do not change)

```yml
server:
  port: 8081
...
agens:
  api:
    base-path: /api
    query-timeout: 600000       # 1000 ms = 1 sec
  elasticsearch:
    host: 127.0.0.1
    port: 9200
    username:
    password:
    page-size: 2500
    vertex-index: agensvertex
    edge-index: agensedge
  spark:
    app-name: es-bitnine
    spark-home: /home/bgmin/Servers/spark
    master-uri: local
    extra-jars: jars/elasticsearch-hadoop-7.7.1.jar,jars/elasticsearch-spark-20_2.11-7.7.1.jar,jars/graphframes-0.8.0-spark2.4-s_2.11.jar
```

### Build & Run

```sh
mvn clean install -DskipTests

java -jar $jarfile --spring.config.name=$cfgname
## or
mvn spring-boot:run --spring.config.location=$cfgfilename
```

### Demo

backend

<img src="https://github.com/skaiworldwide-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-backend-console.png">

frontend

- http://localhost:8081/index.html

<img src="https://github.com/skaiworldwide-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-frontend-web.png">

results after indegree centrality

- elasticsearch/agensvertex/order ==> [search](http://tonyne.iptime.org:9200/agensvertex/_search?q=label:order)

<img src="https://github.com/bitnine-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-shell_indegree_results.png">

