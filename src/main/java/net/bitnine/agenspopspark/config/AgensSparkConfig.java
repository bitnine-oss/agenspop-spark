package net.bitnine.agenspopspark.config;

import net.bitnine.agenspopspark.config.properties.SparkProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
@ComponentScan(basePackages = { "net.bitnine.agenspopspark.service" })
public class AgensSparkConfig {

    @Autowired
    private SparkProperties sparkProperties;

    @Bean
    public SparkConf sparkConf(){
        return new SparkConf()
                .setAppName(sparkProperties.getAppName())
                .setSparkHome(sparkProperties.getSparkHome())
                .setMaster(sparkProperties.getMasterUri())
                .set("spark.executor.memory", sparkProperties.getExecutorMemory())
                .set("spark.driver.memory", sparkProperties.getDriverMemory())
                .set("spark.eventLog.enabled","false");
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
//        SparkConf conf = new SparkConf()
//                .setAppName(sparkProperties.getAppName())
//                .setSparkHome(sparkProperties.getSparkHome())
//                .setMaster(sparkProperties.getMasterUri())
//                .set("spark.executor.memory", sparkProperties.getExecutorMemory())
//                .set("spark.driver.memory", sparkProperties.getDriverMemory())
//                .set("spark.eventLog.enabled","false");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf());
        jsc.sc().setLogLevel("ERROR");

        try {
            Path tempDirWithPrefix = Files.createTempDirectory("agens_");
            jsc.sc().setCheckpointDir(tempDirWithPrefix.toString());
        }catch(Exception e){
            System.out.println("** createTempDirectory Fail:"+e.getMessage());
        }

        return jsc;
    }

}
