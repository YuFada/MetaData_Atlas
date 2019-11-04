package com.dc.cloud.json.runner;

import com.dc.cloud.json.support.HandleHiveJsonClient;
import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import com.dc.cloud.json.support.json.JsonHandlerProperties;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;


@Log4j2
public class HandleHiveDataRunner extends BaseSubscriber<Void> implements CommandLineRunner {

    @Autowired
    private HandleHiveJsonClient jsonClient;

    @Autowired
    private JsonHandlerProperties jsonHandlers;

    private Scheduler scheduler = Schedulers.newParallel("GET-JSON");

    /**
     * //执行入口
     * @param args
     */
    @Override
    public void run(String... args) {
        Collection<Class<? extends HiveDataJsonHandler>> handlerValues = jsonHandlers.getHandlers().values();
        log.info("JSON Handle Start ： " + handlerValues);
        Map<String, Class<? extends HiveDataJsonHandler>> handlers = jsonHandlers.getHandlers();
        callHiveJsonInterface(Collections.synchronizedMap(handlers));
    }

    public void callHiveJsonInterface(Map<String, Class<? extends HiveDataJsonHandler>> handlers){
        HandleHiveDataBaseSubscriber subscriber = new HandleHiveDataBaseSubscriber();
//        Scheduler scheduler = Schedulers.newSingle("GET-JSON");
        Flux.fromIterable(handlers.keySet())
                .doOnNext(handlerPath -> log.info("The jsonHandler [[ " + handlerPath + " ]] is ready to start running"))
                .subscribeOn(scheduler)
                .flatMap(urlKey -> jsonClient.getHiveData(urlKey, handlers.get(urlKey)))
                .doOnError(log::error)
                .next()
//                .block(Duration.ofSeconds(10));
//                .doFinally(signalType -> scheduler.dispose())
                .subscribe(subscriber);
    }


}
