package net.bitnine.agenspopspark.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.*;

@Configuration
@EnableWebMvc
public class AgensWebConfig implements WebMvcConfigurer {

    @Value("${agens.api.base-path}")
    private String basePath="/api";     // fixed

    private static final String[] CLASSPATH_RESOURCE_LOCATIONS = {
            "classpath:/META-INF/resources/", "classpath:/resources/",
            "classpath:/static/" };

    @Override
    public void addCorsMappings(CorsRegistry corsRegistry) {
        corsRegistry.addMapping(basePath+"/**");

        // **참고 https://www.baeldung.com/spring-value-annotation
//		registry.addMapping("/api/**")
//		.allowedOrigins("http://domain2.com")
//		.allowedMethods("PUT", "DELETE")
//		.allowedHeaders("header1", "header2", "header3")
//		.exposedHeaders("header1", "header2")
//		.allowCredentials(false).maxAge(3600);
    }

    // index.html을 찾기 위한 리소스 로케이션 등록
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {

		registry.addResourceHandler("/**", "/webjars/**")
                .addResourceLocations("classpath:/static/", "/webjars/")
                .resourceChain(false);
                // **NOTE: webjars return 404 (not found)
                // ==> https://stackoverflow.com/a/58346519/6811653
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // **NOTE: redirect root url
        registry.addRedirectViewController("/", "/index.html");
    }

}
