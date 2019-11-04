package com.dc.cloud.json.support.config.table;

import com.baomidou.mybatisplus.core.toolkit.Assert;
import com.baomidou.mybatisplus.core.toolkit.PluginUtils;
import com.dc.cloud.json.bean.HiveDynamicTable;
import com.dc.cloud.json.support.config.DynamicTableNameHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.reflection.MetaObject;

public abstract class AbstractDynamicTableNameHandler implements DynamicTableNameHandler {

    private String tableName;

    public AbstractDynamicTableNameHandler(String tableName){
        Assert.notEmpty(tableName, "The tableName must not be empty");
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String dynamicTableName(MetaObject metaObject, String sql, String tableName) {
        Object originalObject = metaObject.getOriginalObject();
        StatementHandler statementHandler = PluginUtils.realTarget(originalObject);
        BoundSql boundSql = statementHandler.getBoundSql();
        HiveDynamicTable hiveData = boundSql.getParameterObject() != null && boundSql.getParameterObject() instanceof HiveDynamicTable ? (HiveDynamicTable) boundSql.getParameterObject() : null;
        Assert.notNull(hiveData, "The hiveData instance must not be null");
        return rebuildDynamicTableName(hiveData, tableName);
    }


    public abstract String rebuildDynamicTableName(HiveDynamicTable dynamicTable,String originTableName);

}
