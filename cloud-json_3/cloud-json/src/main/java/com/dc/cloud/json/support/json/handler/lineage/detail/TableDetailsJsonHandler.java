package com.dc.cloud.json.support.json.handler.lineage.detail;

import com.dc.cloud.json.bean.HiveLineageDetail;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.util.StringUtils;

import java.util.Optional;

public class TableDetailsJsonHandler extends AbstractLineageDetailJsonHandler {

    private static final String dbFieldName = "db";

    public TableDetailsJsonHandler() {
        super("tb");
    }


    @Override
    public void handleJsonData(JSONObject jsonObject, HiveLineageDetail.HiveLineageDetailBuilder builder) {

        if(jsonObject.has(dbFieldName)){

            JSONObject tableFieldJsonObject = jsonObject.optJSONObject(dbFieldName);

            Optional.ofNullable(tableFieldJsonObject.optString(GUID_FIELD_NAME))
                    .filter(StringUtils::hasText)
                    .ifPresent(builder::dbGuid);


        }
    }
}
