package com.dc.cloud.json.support.json.handler;

import com.dc.cloud.json.bean.HiveDynamicTable;
import com.dc.cloud.json.support.event.HandleJsonCompletionEvent;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEvent;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.List;

public abstract class AbstractDataJsonHandler<T extends HiveDynamicTable,E> implements HiveDataJsonHandler<T> {

    private String tableName;

    protected static String ownerFiledName = "owner";
    protected static String createTimeFieldName = "createTime";
    protected static String entitiesFieldName = "entities";
    protected static String descriptionFieldName = "description";

    public AbstractDataJsonHandler(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public abstract void handleJsonData(JSONObject jsonObject, E e);

    @Override
    public ApplicationEvent buildApplicationEvent(List<T> hiveTables) {
        String tableName = hiveTables.get(0).getTableName();
        Tuple2<String,HandleJsonEnum> jsonEnumTuple = eventListenerType(tableName);
        return new HandleJsonCompletionEvent(hiveTables, jsonEnumTuple.getT1(), jsonEnumTuple.getT2());
    }

    public Tuple2<String,HandleJsonEnum> eventListenerType(String tableName){
        return Tuples.of(tableName, HandleJsonEnum.EMPTY);
    }


}
