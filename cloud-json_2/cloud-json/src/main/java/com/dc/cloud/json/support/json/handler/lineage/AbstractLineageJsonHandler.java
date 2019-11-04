package com.dc.cloud.json.support.json.handler.lineage;

import com.dc.cloud.json.bean.HiveData;
import com.dc.cloud.json.runner.HandleHiveDataRunner;
import com.dc.cloud.json.support.HandleHiveJsonClient;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import com.dc.cloud.json.support.json.handler.AbstractDataJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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


    //两种方法实现一种是在service 一种是在这里
    private HiveData.HiveDataBuilder doHandleChildGuid(String tableGuid, Context context, HiveData.HiveDataBuilder builder) {
        if (handleDetailTuple != null) {
//            HandleHiveDataRunner dataRunner = context.get(HandleHiveDataRunner.DEFAULT_DATA_RUNNER_KEY);
//
//            Map<String,  Class<? extends HiveDataJsonHandler>> stringClassMap = Collections.singletonMap(rebuildUrl(tableGuid, handleDetailTuple.getT1()), handleDetailTuple.getT2());
//
//            Map<String,  Class<? extends HiveDataJsonHandler>> map = new ConcurrentHashMap<>();
//
//            map.put(rebuildUrl(tableGuid, handleDetailTuple.getT1()), handleDetailTuple.getT2());
//
//            dataRunner.callHiveJsonInterface(map);
//            rebuildUrl(tableGuid, handleDetailTuple.getT1()), handleDetailTuple.getT2();
            HandleHiveJsonClient jsonClient = context.get(HandleHiveJsonClient.DEFAULT_JSON_CLIENT_RUNNER_KEY);


        }
        return builder;
    }


    private String rebuildUrl(String guid,String url){
        return UriComponentsBuilder.fromUriString(url)
                .queryParam(GUID_FIELD_NAME,guid)
                .build().toUriString();
    }


    @Override
    public Tuple2<String, HandleJsonEnum> eventListenerType(String tableName) {
        return Tuples.of(String.format("The %s is collection Completion", tableName),HandleJsonEnum.HIVE_LINEAGE);
    }

    private List<JSONObject> reduceJsonObject(JSONArray entitiesJsonArray) {

        // CompletableFuture

        List<JSONObject> jsonObjects = new ArrayList<>();

        return Optional.ofNullable(entitiesJsonArray)
                .filter(array -> array.length() > 0)
                .map(jsonArray -> {
                    for (int i = 0; i < entitiesJsonArray.length(); i++) {
                        Optional.ofNullable(entitiesJsonArray.optJSONObject(i)).ifPresent(jsonObjects::add);
                    }
                    return jsonObjects;
                }).orElse(jsonObjects);
    }

//    @Override
//    public List<HiveData> parseHiveData(JSONObject hiveDataJsonObject, ApplicationEventPublisher eventPublisher, Context context) {
//        return Flux.just(hiveDataJsonObject)
//                .filter(jsonObject -> jsonObject.has(entitiesFieldName))
//                .map(jsonObject-> jsonObject.optJSONArray(entitiesFieldName))
//                .flatMapIterable(this::reduceJsonObject)
//                .publishOn(Schedulers.newParallel(""))
//                .map(entitiesJsonObject -> {
//                    String guid = entitiesJsonObject.optString(guidFieldName);
//                    if (entitiesJsonObject.has(attributesFieldName) || StringUtils.hasText(guid)) {
//                        HiveData.HiveDataBuilder builder = HiveData.builder();
//                        Optional.ofNullable(guid)
//                                .filter(StringUtils::hasText)
//                                .ifPresent(builder::guid);
//                        if (entitiesJsonObject.has(attributesFieldName)) {
//                            JSONObject attributesJsonObject = entitiesJsonObject.optJSONObject(attributesFieldName);
//                            handleJsonData(attributesJsonObject, builder);
//                        }
//                        builder.tableName(tableName);
//                        return builder.build();
//                    }
//                    return HiveData.NULL_INSTANCE;
//                }).<List<HiveData>>reduceWith(ArrayList::new, (hiveDataList,hiveData)->{
//                    if(hiveData  == HiveData.NULL_INSTANCE){
//                        hiveDataList.add(hiveData);
//                    }
//                    return hiveDataList;
//                }).zipWith(Mono.just(eventPublisher), this::publishHandleDataEvent);
//    }

}
