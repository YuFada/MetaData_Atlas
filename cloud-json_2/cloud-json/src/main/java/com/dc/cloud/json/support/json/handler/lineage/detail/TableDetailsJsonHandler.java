package com.dc.cloud.json.support.json.handler.lineage.detail;

import com.dc.cloud.json.bean.HiveLineageDetail;
import org.springframework.boot.configurationprocessor.json.JSONObject;

public class TableDetailsJsonHandler extends AbstractLineageDetailJsonHandler {

    public TableDetailsJsonHandler() {
        super("tb");
    }


    @Override
    public void handleJsonData(JSONObject jsonObject, HiveLineageDetail.HiveLineageDetailBuilder builder) {


    }
}
