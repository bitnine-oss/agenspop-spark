package net.bitnine.agenspopspark.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "agens.product")
public class ProductProperties {

    private String name;        // = "agenspop-spark";
    private String version;     // = "1.0";
    private String helloMsg;    // = "agenspop-spark v1.0 (since 2019-10-01)";

}
