package com.dc.cloud.json.support.config;

import com.baomidou.mybatisplus.core.toolkit.Assert;
import com.baomidou.mybatisplus.extension.parsers.DynamicTableNameParser;
import com.baomidou.mybatisplus.extension.parsers.ITableNameHandler;
import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.dc.cloud.json.runner.HandleHiveDataRunner;
import com.dc.cloud.json.support.JsonHandlerGenericConverter;
import com.dc.cloud.json.support.condition.ConditionOnPropertyExist;
import com.dc.cloud.json.support.config.table.ReplaceTableNameHandler;
import com.dc.cloud.json.support.config.table.SimpleTableNameHandler;
import com.dc.cloud.json.support.json.JsonHandlerProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.SmartInstantiationAwareBeanPostProcessor;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.lang.NonNull;

import java.util.*;

/**
 * 如过没有添加任何handler 那入口 runner 默认就不会启动
 */
@Configuration
@EnableConfigurationProperties(JsonHandlerProperties.class)
@ConditionOnPropertyExist("spring.hive.json.handlers")
@Import(AddTableNameRegister.class)
public class MybatisPlusConfig implements SmartInstantiationAwareBeanPostProcessor {

    //启动类
    @Bean
    public HandleHiveDataRunner handleHiveDataRunner() {
        return new HandleHiveDataRunner();
    }

    @Bean
    public DynamicTableAdapter simpleDynamicTableHandler(){
        return new SimpleDynamicTableAdapter();
    }

    @Bean
    public DynamicTableAdapter replaceDynamicTableHandler(){
        return new ReplaceDynamicTableAdapter("%s");
    }



    @Bean
    public JsonHandlerGenericConverter jsonHandlerGenericConverter() {
        return new JsonHandlerGenericConverter();
    }

    @ConditionOnPropertyExist({DynamicTableNameHandler.DYNAMIC_PROPERTY_NAME, "spring.hive.json.handlers"})
    @Configuration
    static class DynamicTableConfiguration implements InitializingBean, BeanFactoryAware {

        private DefaultListableBeanFactory beanRegistry;
        private JsonHandlerProperties handlerProperties;

        @Autowired(required = false)
        private List<DynamicTableAdapter> handlers;

        public DynamicTableConfiguration(JsonHandlerProperties handlerProperties) {
            this.handlerProperties = handlerProperties;
        }

        @Bean
        public PaginationInterceptor paginationInterceptor(ObjectProvider<List<DynamicTableNameHandler>> nameHandlers) {
            PaginationInterceptor paginationInterceptor = new PaginationInterceptor();
            DynamicTableNameParser dynamicTableNameParser = new DynamicTableNameParser();
            HashMap<String, ITableNameHandler> handlerHashMap = new HashMap<>();
            nameHandlers.getIfAvailable(ArrayList::new).forEach(handler -> handlerHashMap.put(handler.getTableName(), handler));
            dynamicTableNameParser.setTableNameHandlerMap(handlerHashMap);
            paginationInterceptor.setSqlParserList(Collections.singletonList(dynamicTableNameParser));
            return paginationInterceptor;
        }

        @Override
        public void setBeanFactory(@NonNull BeanFactory beanFactory) throws BeansException {
            Assert.isTrue(beanFactory instanceof DefaultListableBeanFactory, "The beanFactory must instance of DefaultListableBeanFactory");
            this.beanRegistry = (DefaultListableBeanFactory) beanFactory;
        }

        @Override
        public void afterPropertiesSet() {
            //排序所有handlers
            AnnotationAwareOrderComparator.sort(handlers);

            Optional.ofNullable(handlerProperties)
                    .map(JsonHandlerProperties::getDynamicTableNames)
                    .orElseGet(ArrayList::new)
                    .forEach(tableName ->
                            handlers.stream().filter(handler -> handler.canHandle(tableName))
                                    .findFirst()
                                    .ifPresent(handler -> {
                                        BeanDefinition beanDefinition = genericBeanDefinition(handler.registerTableNameHandlerClass(), tableName);
                                        beanRegistry.registerBeanDefinition(beanDefinition.getBeanClassName() + "_" + tableName, beanDefinition);
                                    }));
        }

        private BeanDefinition genericBeanDefinition(Class<?> beanClass, String tableName) {
            return BeanDefinitionBuilder.genericBeanDefinition(beanClass)
                    .addConstructorArgValue(tableName)
                    .setAutowireMode(AbstractBeanDefinition.AUTOWIRE_NO)
                    .setRole(BeanDefinition.ROLE_INFRASTRUCTURE)
                    .getBeanDefinition();
        }
    }


    static class SimpleDynamicTableAdapter implements DynamicTableAdapter {

        @Override
        public boolean canHandle(String tableName) {
            return true;
        }

        @Override
        public Class<? extends DynamicTableNameHandler> registerTableNameHandlerClass() {
            return SimpleTableNameHandler.class;
        }

        @Override
        public int getOrder() {
            return Integer.MAX_VALUE - 1;
        }
    }


   public static class ReplaceDynamicTableAdapter implements DynamicTableAdapter {

        private String findStr;

        public ReplaceDynamicTableAdapter(String findStr){
            this.findStr = findStr;
        }

        @Override
        public boolean canHandle(String tableName) {
            return tableName.indexOf(findStr) > 0;
        }

        @Override
        public Class<? extends DynamicTableNameHandler> registerTableNameHandlerClass() {
            return ReplaceTableNameHandler.class;
        }

        @Override
        public int getOrder() {
            return Integer.MAX_VALUE - 3;
        }
    }




}
