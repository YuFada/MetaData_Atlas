package com.dc.cloud.json.support.json.handler.lineage.detail;

import com.dc.cloud.json.bean.HiveLineageDetail;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.util.StringUtils;

import java.util.Optional;

public class ColumnDetailsJsonHandler extends AbstractLineageDetailJsonHandler {

    private static final String tableFieldName = "table";

    public ColumnDetailsJsonHandler() {
        super("field");
    }

    @Override
    public void handleJsonData(JSONObject jsonObject, HiveLineageDetail.HiveLineageDetailBuilder builder) {

        if(jsonObject.has(tableFieldName)){

            JSONObject tableFieldJsonObject = jsonObject.optJSONObject(tableFieldName);
            Optional.ofNullable(tableFieldJsonObject.optString(GUID_FIELD_NAME))
                    .filter(StringUtils::hasText)
                    .ifPresent(builder::tbGuid);


        }


    }
}
