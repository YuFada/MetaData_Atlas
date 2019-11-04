package com.dc.cloud.json.support.config;

import com.baomidou.mybatisplus.extension.parsers.ITableNameHandler;

public interface DynamicTableNameHandler extends ITableNameHandler {

    String DYNAMIC_PROPERTY_NAME = "spring.hive.json.dynamic-table-names";

    String getTableName();

}
