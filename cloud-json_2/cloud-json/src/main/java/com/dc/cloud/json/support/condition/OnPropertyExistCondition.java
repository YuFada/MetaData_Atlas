package com.dc.cloud.json.support.condition;

import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@ConditionalOnProperty

//对map类型的校验
class OnPropertyExistCondition extends SpringBootCondition {

    private static final List<String> propertyNames = new CopyOnWriteArrayList<>();

    private static final Map<String, Boolean> propertyNamesCache = new ConcurrentHashMap<>();

    private AtomicBoolean initMark = new AtomicBoolean(false);

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private static final String DEFAULT_FIND_PROFILE = "/application.yml";

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {

        Map<String, Object> attributes = metadata
                .getAnnotationAttributes(ConditionOnPropertyExist.class.getName(), true);

        AnnotationAttributes annotationAttributes = AnnotationAttributes.fromMap(attributes);

        Assert.notNull(annotationAttributes,
                "@ConditionOnPropertyExist annotation attribute least one property value");

        String[] propertyValues = annotationAttributes.getStringArray("value");

        String profilePath = annotationAttributes.getString("profilePath");

        init(context, profilePath);

//        AnnotationUtils.getA(, )

        Assert.isTrue(propertyValues.length > 0,
                "@ConditionOnPropertyExist annotations must specify at " + "least one property name");

        List<String> missingPropertyValues = new ArrayList<>();

        for (String propertyValue : propertyValues) {

            Assert.isTrue(StringUtils.hasText(propertyValue),
                    "@ConditionOnPropertyExist annotations value must not be empty");

            if (propertyNamesCache.get(propertyValue) != null && propertyNamesCache.get(propertyValue))
                continue;
            else if (propertyNamesCache.get(propertyValue) != null && !propertyNamesCache.get(propertyValue))
                missingPropertyValues.add(propertyValue);
            else {
                Pattern p = Pattern.compile(propertyValue + "\\[.*");
                boolean propertyNameExist = propertyNames.stream()
                        .anyMatch(propertyName -> p.matcher(propertyName).matches());
                Optional.of(propertyNameExist).filter(exist-> !exist).ifPresent(exist-> missingPropertyValues.add(propertyValue));
                propertyNamesCache.put(propertyValue, propertyNameExist);
            }

        }

        if (!missingPropertyValues.isEmpty()) {
           return ConditionOutcome.noMatch(ConditionMessage.forCondition(ConditionOnPropertyExist.class, propertyNamesCache)
                    .didNotFind("not found ", "not founds").items(ConditionMessage.Style.QUOTE, missingPropertyValues));
        }

        return ConditionOutcome.match(ConditionMessage.forCondition(ConditionOnPropertyExist.class, propertyNamesCache).because("matched"));

    }

    private void init(ConditionContext conditionContext, String profilePath) {
        synchronized (this) {
            if (!initMark.get()) {

                String proFileName = Optional.ofNullable(profilePath).filter(StringUtils::hasText).orElse(DEFAULT_FIND_PROFILE);
                StandardEnvironment environment = (StandardEnvironment) conditionContext.getEnvironment();
                MutablePropertySources propertySources = environment.getPropertySources();
                propertySources.iterator().forEachRemaining(propertySource -> {
                    if (propertySource.getName().equals(String.format("applicationConfig: [classpath:%s]", proFileName))
                            && propertySource instanceof MapPropertySource) {
                        MapPropertySource source = (MapPropertySource) propertySource;
                        String[] names = source.getPropertyNames();
                        propertyNames.addAll(Arrays.asList(names));
                    }
                });

                initMark.compareAndSet(false, true);
            }

        }

    }

}
