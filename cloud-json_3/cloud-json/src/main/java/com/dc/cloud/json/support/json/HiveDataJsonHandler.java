package com.dc.cloud.json.support.json;

import com.dc.cloud.json.runner.HandleHiveDataRunner;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.CollectionUtils;
import reactor.util.context.Context;
import reactor.util.function.Tuples;

import java.util.List;
import java.util.Optional;


//json处理接口
public interface HiveDataJsonHandler<T> {

    String GUID_FIELD_NAME = "guid";
    String NAME_FIELD_NAME = "name";
    String ATTRIBUTES_FIELD_NAME = "attributes";

    //解析json数据
    List<T> parseHiveData(JSONObject jsonObject, ApplicationEventPublisher eventPublisher, Context context);

    default void publishHandleDataEvent(List<T> hiveTables, ApplicationEventPublisher eventPublisher,Context context) {
        Optional.of(hiveTables)
                .filter(hiveDataList -> !CollectionUtils.isEmpty(hiveDataList))
                .map(this::buildApplicationEvent)
                .map(event-> Tuples.of(event, hiveTables))
                .ifPresent(tuple2 -> {
                    Runnable runnable  = ()-> eventPublisher.publishEvent(tuple2.getT1());
                    HandleHiveDataRunner dataRunner = context.getOrDefault(HandleHiveDataRunner.DEFAULT_DATA_RUNNER_KEY, null);
                    if(dataRunner!= null && dataRunner.getScheduler() != null){
                        dataRunner.getScheduler().schedule(runnable);
                        return;
                    }
                    runnable.run();
                });
    }


    //发布带有解析数据的event
     ApplicationEvent buildApplicationEvent(List<T> hiveTables);


}
