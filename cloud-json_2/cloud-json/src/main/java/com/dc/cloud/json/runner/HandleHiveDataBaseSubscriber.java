package com.dc.cloud.json.runner;

import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Log4j2
public class HandleHiveDataBaseSubscriber extends BaseSubscriber<Void> {

    /**
     * handler keys :  jsonUri
     * handler values : 处理json数据的 接口
     *
     * @param handlers
     */

    private Scheduler scheduler = Schedulers.newSingle("GET_JSON");



    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        request(1);
    }


    @Override
    protected void hookFinally(SignalType type) {
        if (type == SignalType.ON_COMPLETE || type == SignalType.ON_ERROR) {
            scheduler.dispose();
        }
    }

}
