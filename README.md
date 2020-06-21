# agenspop-spark

### summary

es-hadoop 을 이용해 Spark로 그래프 분석을 수행할 수 있는 분석용 백엔드 서버

- build : mvn clean install -DskipTests
- deploy : target/agenspop-spark-0.7.3.jar 
- run : java -jar <jar_file> --spring.config.name=es-config
- demo page : http://<IP:8081>/index.html

### preparations

1) spark-2.4.6-bin-hadoop2.7 설치

- log4j.properties 생성 (template 복사)
- 설치 위치는 spark-home 으로 사용됨

2) es-config.yml 작성

- agens.elasticsearch 의 host, port, vertex-index, edge-index 설정
- agens.spark 의 app-name 과 spark-home 설정
- **<주의>** agens.spark.master-uri 는 'local'로 고정 (변경 금지)

```text
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

### build & run

```
mvn clean install -DskipTests

java -jar $jarfile --spring.config.name=$cfgname
## 또는
mvn spring-boot:run --spring.config.location=$cfgfilename
```

### demo

backend

<img src="https://github.com/bitnine-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-backend-console.png">

frontend

- http://localhost:8081/index.html

<img src="https://github.com/bitnine-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-frontend-web.png">

results after indegree centrality

- elasticsearch/agensvertex/order ==> [search](http://tonyne.iptime.org:9200/agensvertex/_search?q=label:order)

<img src="https://github.com/bitnine-oss/agenspop-spark/blob/master/docs/images/agenspop-spark-shell_indegree_results.png">

