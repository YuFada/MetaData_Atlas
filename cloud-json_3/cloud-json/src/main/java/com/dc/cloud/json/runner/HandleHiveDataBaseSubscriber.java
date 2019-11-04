package com.dc.cloud.json.runner;

import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.MonoSink;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Log4j2
public class HandleHiveDataBaseSubscriber extends BaseSubscriber<List> {

    private MonoSink<Void> monoSink;

    private CountDownLatch countDownLatch;

    public HandleHiveDataBaseSubscriber(MonoSink<Void> monoSink, CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        this.monoSink = monoSink;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        request(1);
    }

    @Override
    protected void hookOnComplete() {
        try {
            countDownLatch.await();
            monoSink.success();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    protected void hookOnError(Throwable throwable) {
        monoSink.error(throwable);
    }
}
