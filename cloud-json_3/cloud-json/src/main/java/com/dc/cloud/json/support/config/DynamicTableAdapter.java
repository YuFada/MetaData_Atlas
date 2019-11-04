package com.dc.cloud.json.support.config;

import org.springframework.core.Ordered;

public interface DynamicTableAdapter extends Ordered {

    boolean canHandle(String tableName);

    Class<? extends DynamicTableNameHandler> registerTableNameHandlerClass();


}
