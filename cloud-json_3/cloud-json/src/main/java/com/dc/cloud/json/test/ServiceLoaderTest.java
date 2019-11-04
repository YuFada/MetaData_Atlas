package com.dc.cloud.json.test;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Log4j2
public class ServiceLoaderTest {

    public static void main(String[] args) {

        Flux.just("123",2,3,4,5,6)
                .doOnNext(s-> log.info("2z"))
                .map(s-> Mono.empty())
                .next()
                .doOnNext(s-> log.info("我杂这里"))
                .subscribe(s-> log.info("我杂这里1"));

        ExecutorService executorService = Executors.newFixedThreadPool(4);


        for (int i = 0; i < 100; i++) {
            int finalI = i;
            executorService.submit(()-> log.info(System.currentTimeMillis()));
        }
        executorService.shutdown();
        System.out.println(System.currentTimeMillis());
    }

}
