package com.dc.cloud.json.support.json.handler.lineage.detail;

import com.dc.cloud.json.bean.HiveLineageDetail;
import org.springframework.boot.configurationprocessor.json.JSONObject;


public class DBDetailsJsonHandler extends AbstractLineageDetailJsonHandler {

    public DBDetailsJsonHandler() {
        super("db");
    }

    @Override
    public void handleJsonData(JSONObject jsonObject, HiveLineageDetail.HiveLineageDetailBuilder builder) {



    }
}
