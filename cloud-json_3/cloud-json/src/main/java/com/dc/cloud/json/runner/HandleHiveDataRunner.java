package com.dc.cloud.json.runner;

import com.dc.cloud.json.support.HandleHiveJsonClient;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import com.dc.cloud.json.support.json.JsonHandlerProperties;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Log4j2
public class HandleHiveDataRunner extends BaseSubscriber<List> implements CommandLineRunner, InitializingBean {

    @Getter
    @Autowired
    private HandleHiveJsonClient jsonClient;

    @Autowired
    private JsonHandlerProperties jsonHandlers;

    @Getter
    private Scheduler scheduler ;

    //子调用有三个
    @Getter
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ExecutorService executorService;

    public static final String DEFAULT_DATA_RUNNER_KEY = HandleHiveDataRunner.class.getName() + "_runner";

    @Override
    public void afterPropertiesSet() throws Exception {
        executorService = Executors.newFixedThreadPool(6);
        scheduler = Schedulers.fromExecutor(executorService);
    }

    /**
     * //执行入口
     *
     * @param args
     */
    @Override
    public void run(String... args) {
        Collection<Class<? extends HiveDataJsonHandler>> handlerValues = jsonHandlers.getHandlers().values();
        log.info("JSON Handle Start ： " + handlerValues);
        Map<String, Class<? extends HiveDataJsonHandler>> handlers = jsonHandlers.getHandlers();
//        Mono.<Void>create(monoSink -> {
//            HandleHiveDataBaseSubscriber handler = new HandleHiveDataBaseSubscriber(monoSink, countDownLatch);
//            monoSink.onDispose(scheduler);
//            universallyHiveJson(Collections.synchronizedMap(handlers))
//                    .doOnNext(s -> log.info("callChildHiveJsonInterface1 含着类啊 " + s)).subscribe(handler);
//        }).doOnError(log::error).subscribe();
//                .doFinally(s-> {
//            try {
//                synchronized (this){
//                    this.wait();
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        })
        universallyHiveJson(Collections.synchronizedMap(handlers)).subscribe(this);
    }


    //通用调用
    public Flux<List> universallyHiveJson(Map<String, Class<? extends HiveDataJsonHandler>> handlers) {
        return getJsonData(handlers)
                .subscriberContext(context -> context.put(DEFAULT_DATA_RUNNER_KEY, this))
                .doOnError(log::error);
    }

    /**
     * handler keys :  jsonUri
     * handler values : 处理json数据的 接口
     *
     * @param
     */
    public Flux<List> getJsonData(Map<String, Class<? extends HiveDataJsonHandler>> handlers) {
        return jsonClient.getHiveData(Mono.just(handlers),scheduler)
                .subscribeOn(scheduler);
    }

    public Flux<List> getJsonDataWithDefaultWebClient(Map<String, Class<? extends HiveDataJsonHandler>> handlers) {
        return jsonClient.getHiveData(Mono.just(handlers),scheduler)
                .subscribeOn(scheduler);
    }


    @Override
    protected void hookFinally(SignalType type) {
        log.info("Wait all Threads " + "控制");
        try {
            countDownLatch.await();
            this.executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
