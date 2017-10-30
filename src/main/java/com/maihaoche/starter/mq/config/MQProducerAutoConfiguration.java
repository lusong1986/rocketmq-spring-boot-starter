package com.maihaoche.starter.mq.config;

import java.util.Map;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.maihaoche.starter.mq.annotation.MQProducer;
import com.maihaoche.starter.mq.base.AbstractMQProducer;

/**
 * Created by yipin on 2017/6/29.
 * 自动装配消息生产者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQProducerAutoConfiguration extends MQBaseAutoConfiguration {

    private DefaultMQProducer producer;

    @PostConstruct
    public void init() throws Exception {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQProducer.class);
        //对于仅仅只存在消息消费者的项目，无需构建生产者
        if(CollectionUtils.isEmpty(beans)){
            return;
        }
        if(producer == null) {
            if(StringUtils.isEmpty(mqProperties.getProducerGroup())) {
                throw new RuntimeException("producer group must be defined");
            }
            if(StringUtils.isEmpty(mqProperties.getNameServerAddress())) {
                throw new RuntimeException("name server address must be defined");
            }
            producer = new DefaultMQProducer(mqProperties.getProducerGroup());
            producer.setNamesrvAddr(mqProperties.getNameServerAddress());
            producer.start();
        }
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            publishProducer(entry.getKey(), entry.getValue());
        }
    }

    private void publishProducer(String beanName, Object bean) throws Exception {
        if(!AbstractMQProducer.class.isAssignableFrom(bean.getClass())) {
            throw new RuntimeException(beanName + " - producer未继承AbstractMQProducer");
        }
        AbstractMQProducer abstractMQProducer = (AbstractMQProducer) bean;
        abstractMQProducer.setProducer(producer);
        // begin build producer level topic
        MQProducer mqProducer = applicationContext.findAnnotationOnBean(beanName, MQProducer.class);
        String topic = mqProducer.topic();
        if(!StringUtils.isEmpty(topic)) {
            String transTopic = applicationContext.getEnvironment().getProperty(topic);
            if(StringUtils.isEmpty(transTopic)) {
                abstractMQProducer.setTopic(topic);
            } else {
                abstractMQProducer.setTopic(transTopic);
            }
        }
        // begin build producer level tag
        String tag = mqProducer.tag();
        if(!StringUtils.isEmpty(tag)) {
            String transTag = applicationContext.getEnvironment().getProperty(tag);
            if(StringUtils.isEmpty(transTag)) {
                abstractMQProducer.setTag(tag);
            } else {
                abstractMQProducer.setTag(transTag);
            }
        }
        log.info(String.format("%s is ready to produce message", beanName));
    }
}