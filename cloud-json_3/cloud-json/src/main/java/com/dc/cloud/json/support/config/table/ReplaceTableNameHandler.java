package com.dc.cloud.json.support.config.table;

import com.dc.cloud.json.bean.HiveDynamicTable;

public class ReplaceTableNameHandler extends AbstractDynamicTableNameHandler {

    public ReplaceTableNameHandler(String tableName) {
        super(tableName);
    }

    @Override
    public String rebuildDynamicTableName(HiveDynamicTable dynamicTable, String originTableName) {
        dynamicTable.getTableName();
        return String.format(originTableName,dynamicTable.getTableName());
    }
}
