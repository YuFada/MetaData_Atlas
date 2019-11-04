package com.dc.cloud.json.support.json.handler.lineage.detail;

import com.dc.cloud.json.bean.HiveLineageDetail;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.handler.AbstractDataJsonHandler;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.configurationprocessor.json.JSONArray;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.StringUtils;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Log4j2
public abstract class AbstractLineageDetailJsonHandler extends AbstractDataJsonHandler<HiveLineageDetail, HiveLineageDetail.HiveLineageDetailBuilder> {

    private static String locationFiledName = "location";
    private static String createByFiledName = "createdBy";
    private static String updatedByFiledName = "updatedBy";
    private static String updateTimeFiledName = "updateTime";

    AbstractLineageDetailJsonHandler(String tableName) {
        super(tableName);
    }

    @Override
    public List<HiveLineageDetail> parseHiveData(JSONObject hiveDataJsonObject, ApplicationEventPublisher eventPublisher, Context context) {
        //一样的部分在这里处理了 createTime 子类处理

        log.info("子查询url开始解析json数据");

        List<HiveLineageDetail> hiveTables = new ArrayList<>();

        if (hiveDataJsonObject.has(entitiesFieldName)) {

            JSONArray entitiesJsonArray = hiveDataJsonObject.optJSONArray(entitiesFieldName);

            for (int i = 0; i < entitiesJsonArray.length(); i++) {

                Optional.ofNullable(entitiesJsonArray.optJSONObject(i))
                        .ifPresent(entitiesJsonObject -> {

                            String guid = entitiesJsonObject.optString(GUID_FIELD_NAME);

                            if (entitiesJsonObject.has(GUID_FIELD_NAME) || StringUtils.hasText(guid)) {

                                HiveLineageDetail.HiveLineageDetailBuilder builder = HiveLineageDetail.builder();

                                Optional.ofNullable(entitiesJsonObject.optString(GUID_FIELD_NAME))
                                        .filter(StringUtils::hasText)
                                        .ifPresent(builder::guid);

                                Optional.ofNullable(entitiesJsonObject.optString(createByFiledName))
                                        .filter(StringUtils::hasText)
                                        .ifPresent(builder::createBy);

                                Optional.ofNullable(entitiesJsonObject.optString(updatedByFiledName))
                                        .filter(StringUtils::hasText)
                                        .ifPresent(builder::updateBy);

                                Optional.of(entitiesJsonObject.optLong(createTimeFieldName))
                                        .filter(createTime -> createTime != 0L)
                                        .ifPresent(createTime -> builder.createTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(createTime), ZoneId.systemDefault())));

                                Optional.of(entitiesJsonObject.optLong(updateTimeFiledName))
                                        .filter(updateTime -> updateTime != 0L)
                                        .ifPresent(updateTime -> builder.updateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(updateTime), ZoneId.systemDefault())));


                                if (entitiesJsonObject.has(ATTRIBUTES_FIELD_NAME)) {

                                    JSONObject attributesJsonObject = entitiesJsonObject.optJSONObject(ATTRIBUTES_FIELD_NAME);

                                    Optional.ofNullable(attributesJsonObject.optString(ownerFiledName))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::owner);

                                    Optional.ofNullable(attributesJsonObject.optString(NAME_FIELD_NAME))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::name);

                                    Optional.ofNullable(attributesJsonObject.optString(descriptionFieldName))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::des);

                                    Optional.ofNullable(attributesJsonObject.optString(locationFiledName))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::location);


                                    handleJsonData(attributesJsonObject, builder);
                                }

                                hiveTables.add(builder.build().tableName(getTableName()));
                            }

                        });
            }
        }

        return hiveTables;
    }


    @Override
    public Tuple2<String, HandleJsonEnum> eventListenerType(String tableName) {
        return Tuples.of(String.format("The %s details is collection Completion", tableName),HandleJsonEnum.HIVE_LINEAGE_DETAIL);
    }

    public abstract void handleJsonData(JSONObject jsonObject, HiveLineageDetail.HiveLineageDetailBuilder builder);

}
