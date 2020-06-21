package net.bitnine.agenspopspark.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
@ConfigurationProperties(prefix = "agens.spark")
public class SparkProperties {

    @NotBlank
    private String appName = "es-bitnine";
    @NotBlank
    private String sparkHome;
    @NotBlank
    private String masterUri = "local";
    // **NOTE: At build, jar files will copy to target/jars path
    @NotBlank
    private String extraJars = "jars/elasticsearch-hadoop-7.7.1.jar,jars/elasticsearch-spark-20_2.11-7.7.1.jar,jars/graphframes-0.8.0-spark2.4-s_2.11.jar";

    // set options at spark-default.conf
    // private String executorMemory = "2g";
    // private String driverMemory = "2g";
}