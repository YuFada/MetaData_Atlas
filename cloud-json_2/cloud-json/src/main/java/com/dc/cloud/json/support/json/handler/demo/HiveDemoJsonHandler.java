package com.dc.cloud.json.support.json.handler.demo;

import com.dc.cloud.json.bean.HiveDemo;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.handler.AbstractDataJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEventPublisher;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.List;

public class HiveDemoJsonHandler extends AbstractDataJsonHandler<HiveDemo,HiveDemo.HiveDemoBuilder> {

    public HiveDemoJsonHandler() {
        super("");
    }

    @Override
    public void handleJsonData(JSONObject jsonObject, HiveDemo.HiveDemoBuilder hiveDemoBuilder) {

    }

    //{"name":"123","age":"23"}

    @Override
    public List<HiveDemo> parseHiveData(JSONObject jsonObject, ApplicationEventPublisher eventPublisher, Context context) {

        HiveDemo.HiveDemoBuilder builder = HiveDemo.builder();

        String name = jsonObject.optString(NAME_FIELD_NAME);
        String age = jsonObject.optString("AGE");

        List<HiveDemo> hiveDemos = new ArrayList<>();

        builder.name(name);

        builder.age(age);

        HiveDemo build = builder.build();

        hiveDemos.add(builder.build());

        return hiveDemos;
    }



    //发布event事件  带有解析的json数据

}
