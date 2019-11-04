package com.dc.cloud.json.support.json.handler.table;

import com.dc.cloud.json.bean.HiveLineageTable;
import com.dc.cloud.json.support.event.HandleJsonCompletionEvent;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.util.StringUtils;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class LineageTableJsonHandler implements HiveDataJsonHandler<HiveLineageTable> {

    private static final String baseEntityGuidFieldName = "baseEntityGuid";
    private static final String qualifiedNameFieldName = "qualifiedName";
    private static final String guidEntityMapFieldName = "guidEntityMap";
    private static final String relationsFieldName = "relations";
    private static final String fromEntityIdFieldName = "fromEntityId";
    private static final String toEntityIdFieldName = "toEntityId";

    @Override
    @SuppressWarnings("unchecked")
    public List<HiveLineageTable> parseHiveData(JSONObject jsonObject, ApplicationEventPublisher eventPublisher, Context context) {

        List<HiveLineageTable> lineageTables = new ArrayList<>();

        String baseEntityGuid = jsonObject.optString(baseEntityGuidFieldName);

        if (StringUtils.hasText(baseEntityGuid)) {

            JSONObject guidEntityJsonObject = Optional.ofNullable(jsonObject.optJSONObject(guidEntityMapFieldName))
                    .orElseGet(JSONObject::new);

            Iterator<String> keys = guidEntityJsonObject.keys();

            keys.forEachRemaining(entityKey -> {

                HiveLineageTable.HiveLineageTableBuilder builder = HiveLineageTable.builder();

                builder.baseEntityGuid(baseEntityGuid);

                JSONObject tableOperationJsonObject = guidEntityJsonObject.optJSONObject(entityKey);

                Optional.ofNullable(tableOperationJsonObject.optString(GUID_FIELD_NAME))
                        .filter(StringUtils::hasText)
                        .ifPresent(builder::guid);

                Optional.ofNullable(tableOperationJsonObject.optJSONObject(ATTRIBUTES_FIELD_NAME))
                        .ifPresent(attributesJsonObject -> buildGuidEntityChild(attributesJsonObject, builder));

                builder.tableName("lineage_table");

                lineageTables.add(builder.build());

            });

        }

        Optional.ofNullable(jsonObject.optJSONArray(relationsFieldName))
                .filter(jsonArray -> jsonArray.length() > 0)
                .ifPresent(jsonArray -> {

                    List<HiveLineageTable> tables = new ArrayList<>();
                    for (int i = 0; i < jsonArray.length(); i++) {
                        Optional.ofNullable(jsonArray.optJSONObject(i))
                                .ifPresent(entitiesJsonObject -> {
                                    HiveLineageTable.HiveLineageTableBuilder builder = HiveLineageTable.builder();

                                    Optional.ofNullable(entitiesJsonObject.optString(fromEntityIdFieldName))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::fromEntityId);

                                    Optional.ofNullable(entitiesJsonObject.optString(toEntityIdFieldName))
                                            .filter(StringUtils::hasText)
                                            .ifPresent(builder::toEntityId);

                                    builder.tableName("table_relations");

                                    Optional.of(builder.build())
                                            .filter(HiveLineageTable::checkEntityId)
                                            .ifPresent(tables::add);

                                });
                    }

                    publishHandleDataEvent(tables, eventPublisher,context);

                });


        return lineageTables;
    }


    private void buildGuidEntityChild(JSONObject attributesJsonObject, HiveLineageTable.HiveLineageTableBuilder builder) {

        Optional.ofNullable(attributesJsonObject.optString(NAME_FIELD_NAME))
                .filter(StringUtils::hasText)
                .ifPresent(builder::name);

        Optional.ofNullable(attributesJsonObject.optString(qualifiedNameFieldName))
                .filter(StringUtils::hasText)
                .ifPresent(builder::qualifiedName);

    }

    @Override
    public ApplicationEvent buildApplicationEvent(List<HiveLineageTable> hiveTables) {
        String tableName = hiveTables.get(0).getTableName();
        return new HandleJsonCompletionEvent(hiveTables, String.format("The %s is collection Completion", tableName), HandleJsonEnum.HIVE_RELATION);
    }
}
