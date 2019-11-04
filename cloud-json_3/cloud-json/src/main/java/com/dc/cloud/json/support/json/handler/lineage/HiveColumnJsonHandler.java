package com.dc.cloud.json.support.json.handler.lineage;

import com.dc.cloud.json.bean.HiveData;
import com.dc.cloud.json.support.json.handler.lineage.detail.ColumnDetailsJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.util.StringUtils;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class HiveColumnJsonHandler extends AbstractLineageJsonHandler {

    private static final String DEFAULT_TABLE_NAME = "field";

    public HiveColumnJsonHandler() {
        super(DEFAULT_TABLE_NAME,Tuples.of("http://node101:21000/api/atlas/v2/entity/bulk?", ColumnDetailsJsonHandler.class));
    }


    @Override
    public void handleJsonData(JSONObject attributesJsonObject, HiveData.HiveDataBuilder builder) {

        Optional.ofNullable(attributesJsonObject.optString(ownerFiledName))
                .filter(StringUtils::hasText)
                .ifPresent(builder::owner);

        long createTime = attributesJsonObject.optLong(createTimeFieldName);
        Optional.of(createTime)
                .filter(time -> createTime != 0L)
                .ifPresent(time -> builder.createTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(createTime), ZoneId.systemDefault())));

        Optional.ofNullable(attributesJsonObject.optString(NAME_FIELD_NAME))
                .filter(StringUtils::hasText)
                .ifPresent(builder::name);
    }




}
