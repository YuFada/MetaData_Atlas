package com.dc.cloud.json;

import lombok.extern.log4j.Log4j2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log4j2
@RunWith(JUnit4.class)
public class ReactorTest {

    @Test
    public void test1(){
        String key = "message";
        Flux.just("Hello")
                .flatMap( s -> Mono.subscriberContext()
                        .map( ctx -> s + " " + ctx.get(key)))
                .subscriberContext(ctx -> ctx.put(key, "World"))
                .flatMap(s-> {
                    return Mono.subscriberContext()
                            .doOnNext(log::info);
                })
                .subscribe(log::info);

    }

    @Test
    public void test2(){
        WebTestClient
                .bindToServer()
                .baseUrl("http://localhost:8080")
                .build()
                .post()
                .uri("/resource")
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("Content-Type", "application/json")
                .expectBody().isEmpty();
    }


}
