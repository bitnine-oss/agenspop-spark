# agenspop-spark

### summary

es-hadoop 을 이용해 Spark로 그래프 분석을 수행할 수 있는 분석용 백엔드 서버

- build : mvn clean install -DskipTests
- deploy : target/agenspop-spark-0.7.3.jar 
- run : java -jar <jar_file> --spring.config.name=es-config
- demo page : http://<IP:8081>/index.html
