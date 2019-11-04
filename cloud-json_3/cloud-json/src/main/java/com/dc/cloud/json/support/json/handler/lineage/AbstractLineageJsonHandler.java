package com.dc.cloud.json.support.json.handler.lineage;

import com.dc.cloud.json.bean.HiveData;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import com.dc.cloud.json.support.json.handler.AbstractDataJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.StringUtils;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractLineageJsonHandler extends AbstractDataJsonHandler<HiveData, HiveData.HiveDataBuilder> {

    private Tuple2<String, Class<? extends HiveDataJsonHandler>> handleDetailTuple;

    public AbstractLineageJsonHandler(String tableName, Tuple2<String, Class<? extends HiveDataJsonHandler>> handleDetailTuple) {
        super(tableName);
        this.handleDetailTuple = handleDetailTuple;
    }

    @Override
    public List<HiveData> parseHiveData(JSONObject hiveDataJsonObject, ApplicationEventPublisher eventPublisher, Context context) {

        List<HiveData> hiveTables = new ArrayList<>();

        if (hiveDataJsonObject.has(entitiesFieldName)) {

            JSONArray entitiesJsonArray = hiveDataJsonObject.optJSONArray(entitiesFieldName);

            for (int i = 0; i < entitiesJsonArray.length(); i++) {

                Optional.ofNullable(entitiesJsonArray.optJSONObject(i))
                        .ifPresent(entitiesJsonObject -> {

                            String guid = entitiesJsonObject.optString(GUID_FIELD_NAME);

                            if (entitiesJsonObject.has(GUID_FIELD_NAME) || StringUtils.hasText(guid)) {

                                HiveData.HiveDataBuilder builder = HiveData.builder();

                                Optional.ofNullable(guid)
                                        .filter(StringUtils::hasText)
                                        .ifPresent(builder::guid);

                                if (entitiesJsonObject.has(ATTRIBUTES_FIELD_NAME)) {
                                    JSONObject attributesJsonObject = entitiesJsonObject.optJSONObject(ATTRIBUTES_FIELD_NAME);
                                    handleJsonData(attributesJsonObject, builder);
                                }

                                //设置这个属性 就是要在service监听的时候执行
                                builder.requestJsonTuple(Tuples.of(guid,handleDetailTuple.getT1(),handleDetailTuple.getT2()));

                                hiveTables.add(builder.build().tableName(getTableName()));

                            }

                        });
            }
        }



        return hiveTables;

    }


    @Override
    public Tuple2<String, HandleJsonEnum> eventListenerType(String tableName) {
        return Tuples.of(String.format("The %s is collection Completion", tableName),HandleJsonEnum.HIVE_LINEAGE);
    }

}
