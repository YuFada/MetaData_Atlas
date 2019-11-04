package com.dc.cloud.json;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@MapperScan("com.dc.cloud.json.dao")
@SpringBootApplication
public class CloudJsonApplication {

    public static void main(String[] args) {
        SpringApplicationBuilder applicationBuilder = new SpringApplicationBuilder(CloudJsonApplication.class);

        applicationBuilder.web(WebApplicationType.NONE);

        applicationBuilder.build().run(args);

    }

}
