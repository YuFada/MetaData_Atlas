package com.dc.cloud.json.support.json.handler.lineage;

import com.dc.cloud.json.bean.HiveData;
import com.dc.cloud.json.support.json.handler.lineage.detail.ColumnDetailsJsonHandler;
import com.dc.cloud.json.support.json.handler.lineage.detail.DBDetailsJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.util.StringUtils;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

public class HiveDBJsonHandler extends AbstractLineageJsonHandler {

    private static final String DEFAULT_TABLE_NAME = "db";

    public HiveDBJsonHandler() {
        super(DEFAULT_TABLE_NAME, Tuples.of("http://node101:21000/api/atlas/v2/entity/bulk?", DBDetailsJsonHandler.class));
    }

    @Override
    public void handleJsonData(JSONObject attributesJsonObject, HiveData.HiveDataBuilder builder) {

        Optional.ofNullable(attributesJsonObject.optString(ownerFiledName))
                .filter(StringUtils::hasText)
                .ifPresent(builder::owner);

        long createTime = attributesJsonObject.optLong(createTimeFieldName, Long.MIN_VALUE);

        Optional.of(createTime)
                .filter(time -> createTime != Long.MIN_VALUE)
                .ifPresent(time -> builder.createTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(createTime), ZoneId.systemDefault())));

        Optional.ofNullable(attributesJsonObject.optString(NAME_FIELD_NAME))
                .filter(StringUtils::hasText)
                .ifPresent(builder::name);

        Optional.ofNullable(attributesJsonObject.optString(descriptionFieldName))
                .filter(StringUtils::hasText)
                .ifPresent(builder::des);

    }
}
