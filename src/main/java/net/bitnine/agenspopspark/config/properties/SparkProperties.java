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

    private String executorMemory = "2g";
    private String driverMemory = "2g";

}