package com.dc.cloud.json.support.condition;

import org.springframework.context.annotation.Conditional;

import java.lang.annotation.*;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(OnPropertyExistCondition.class)
//spring默认无法对map校验  RelaxedPropertyResolver 类被移除掉
public @interface ConditionOnPropertyExist {

    String []value();

    String  profilePath() default "";

}
