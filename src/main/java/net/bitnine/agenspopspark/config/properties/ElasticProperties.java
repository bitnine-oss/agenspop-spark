package net.bitnine.agenspopspark.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

@Getter
@Setter
@ConfigurationProperties(prefix = "agens.elasticsearch")
public class ElasticProperties {

    @NotBlank
    private String host;        // = "localhost";
    @NotBlank
    private int port;           // = 9200;

    private String username;    // = null;
    private String password;    // = null;

    private int pageSize;       // = 2500;

    @NotBlank
    private String vertexIndex; // = "elasticvertex";
    @NotBlank
    private String edgeIndex;   // = "elasticedge";

}
