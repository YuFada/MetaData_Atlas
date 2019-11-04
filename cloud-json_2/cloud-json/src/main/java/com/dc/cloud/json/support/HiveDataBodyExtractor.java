package com.dc.cloud.json.support;

import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ReactiveHttpInputMessage;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.BodyExtractor;
import org.springframework.web.reactive.function.BodyExtractors;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public class HiveDataBodyExtractor implements BodyExtractor<Mono<Void>, ReactiveHttpInputMessage> {

    private static final Map<String, HiveDataJsonHandler> jsonHandles = new ConcurrentHashMap<>();

    @Setter
    @Getter
    private Class<? extends HiveDataJsonHandler> handleHiveDataClass;

    private ApplicationEventPublisher eventPublisher;

    private HiveDataBodyExtractor(Class<? extends HiveDataJsonHandler> handleHiveDataClass, ApplicationEventPublisher applicationEventPublisher) {
        this.handleHiveDataClass = handleHiveDataClass;
        this.eventPublisher = applicationEventPublisher;
    }

    //获取所有json的处理类 (没啥用 就是为了生成单例，SpringFactoriesLoader也一样，以及提醒类注入)
    static {
        ServiceLoader<HiveDataJsonHandler> loaders = ServiceLoader.load(HiveDataJsonHandler.class);
        loaders.forEach(handleHiveData -> jsonHandles.put(handleHiveData.getClass().getName(), handleHiveData));
    }


    //为了做成reactor类型
    @Override
    public Mono<Void> extract(@NonNull ReactiveHttpInputMessage inputMessage, @NonNull Context context) {

        ParameterizedTypeReference<Map<String, Object>> type = new ParameterizedTypeReference<Map<String, Object>>() {
        };

        BodyExtractor<Mono<Map<String, Object>>, ReactiveHttpInputMessage> delegate = BodyExtractors.toMono(type);

        return delegate.extract(inputMessage, context)
                .flatMap(this::hiveDataParse);

    }

    @SuppressWarnings("unchecked")
    private  Mono<Void> hiveDataParse(Map<String, Object> jsonData) {
        HiveDataJsonHandler handleHiveDataJson = jsonHandles.get(this.getHandleHiveDataClass().getName());

        Assert.notNull(handleHiveDataJson, "The hiveDataHandler is not in default handlers");

        JSONObject jsonObject = new JSONObject(jsonData);

        AtomicReference<String> jsonUrl = new AtomicReference<>("");

        return Mono.subscriberContext()
                .doOnNext(context -> jsonUrl.set(context.get(HandleHiveJsonClient.REQUEST_HANDLE_JSON_URL)))
                .onErrorMap(JSONException.class, e-> new JsonParseException("JSON parse error from url [[ " + jsonUrl.toString() + "]], " +
                        "because: " + e.getMessage(), e))
                .map(context-> handleHiveDataJson.parseHiveData(jsonObject, eventPublisher,context))
                .doOnNext(list -> handleHiveDataJson.publishHandleDataEvent(list, eventPublisher))
                .doOnError(JsonParseException.class, log::error)
                .onErrorResume(JsonParseException.class,e-> Mono.empty())
                .then();

    }

    static <T extends HiveDataJsonHandler> BodyExtractor<Mono<Void>, ReactiveHttpInputMessage> hiveTableBodyExtractor(Class<T> jsonClass, ApplicationEventPublisher publisher) {
        Assert.notNull(jsonClass, "The handleHiveData class mute not be null");
        return new HiveDataBodyExtractor(jsonClass, publisher);
    }


}
