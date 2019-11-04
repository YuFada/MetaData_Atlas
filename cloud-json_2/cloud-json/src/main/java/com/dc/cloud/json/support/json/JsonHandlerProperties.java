package com.dc.cloud.json.support.json;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "spring.hive.json")
public class JsonHandlerProperties {

    private Map<String,Class<? extends HiveDataJsonHandler>> handlers;

    private List<String> dynamicTableNames;


}
