package com.dc.cloud.json.support.config.table;

import com.dc.cloud.json.bean.HiveDynamicTable;

public class SimpleTableNameHandler extends AbstractDynamicTableNameHandler {

    public SimpleTableNameHandler(String tableName) {
        super(tableName);
    }

    @Override
    public String rebuildDynamicTableName(HiveDynamicTable dynamicTable, String originTableName) {
        String tableNameSuffix = dynamicTable.getTableName();
        return originTableName + tableNameSuffix;
    }
}
