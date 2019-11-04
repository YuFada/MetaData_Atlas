package com.dc.cloud.json.service;

//import com.dc.cloud.json.dao.HiveDataDao;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dc.cloud.json.bean.HiveDemo;
import com.dc.cloud.json.bean.HiveLineageTable;
import com.dc.cloud.json.dao.HiveDemoDao;
import com.dc.cloud.json.dao.HiveLineageDao;
import com.dc.cloud.json.support.event.HandleJsonCompletionEvent;
import com.dc.cloud.json.support.event.HandleJsonEnum;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.ApplicationListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;


@Log4j2
@Service
public class HandleDemoService extends ServiceImpl<HiveDemoDao, HiveDemo>
        implements ApplicationListener<HandleJsonCompletionEvent> {

    @Override
    public void onApplicationEvent(@NonNull HandleJsonCompletionEvent event) {
        if(event.getHandleJsonEnum().equals(HandleJsonEnum.HIVE_DEMO)){
            this.saveBatch(event.getHiveData());
            log.info(event.getMessage());
        }
    }

}
