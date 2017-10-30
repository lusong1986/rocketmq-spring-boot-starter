package com.maihaoche.starter.mq.config;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.maihaoche.starter.mq.annotation.MQConsumer;
import com.maihaoche.starter.mq.base.AbstractMQPullConsumer;
import com.maihaoche.starter.mq.base.AbstractMQPushConsumer;

/**
 * Created by suclogger on 2017/6/28. 自动装配消息消费者
 */
@Slf4j
@Configuration
@ConditionalOnBean(MQBaseAutoConfiguration.class)
public class MQConsumerAutoConfiguration extends MQBaseAutoConfiguration {
	@PostConstruct
	public void init() throws Exception {
		Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQConsumer.class);
		for (Map.Entry<String, Object> entry : beans.entrySet()) {
			publishConsumer(entry.getKey(), entry.getValue());
		}
	}

	private void publishConsumer(String beanName, Object bean) throws Exception {
		MQConsumer mqConsumer = applicationContext.findAnnotationOnBean(beanName, MQConsumer.class);
		if (StringUtils.isEmpty(mqProperties.getNameServerAddress())) {
			throw new RuntimeException("name server address must be defined");
		}
		if (StringUtils.isEmpty(mqConsumer.consumerGroup())) {
			throw new RuntimeException("consumer's consumerGroup must be defined");
		}
		if (StringUtils.isEmpty(mqConsumer.topic())) {
			throw new RuntimeException("consumer's topic must be defined");
		}
		if (!AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())
				&& !AbstractMQPullConsumer.class.isAssignableFrom(bean.getClass())) {
			throw new RuntimeException(bean.getClass().getName() + " - consumer未实现Consumer抽象类");
		}

		String consumerGroup = applicationContext.getEnvironment().getProperty(mqConsumer.consumerGroup());
		if (StringUtils.isEmpty(consumerGroup)) {
			consumerGroup = mqConsumer.consumerGroup();
		}
		String topic = applicationContext.getEnvironment().getProperty(mqConsumer.topic());
		if (StringUtils.isEmpty(topic)) {
			topic = mqConsumer.topic();
		}

		// 配置push consumer
		if (AbstractMQPushConsumer.class.isAssignableFrom(bean.getClass())) {
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
			consumer.setNamesrvAddr(mqProperties.getNameServerAddress());
			consumer.setMessageModel(MessageModel.valueOf(mqConsumer.messageMode()));
			consumer.subscribe(topic, StringUtils.join(mqConsumer.tag(), "||"));
			consumer.setInstanceName(UUID.randomUUID().toString());
			final AbstractMQPushConsumer abstractMQPushConsumer = (AbstractMQPushConsumer) bean;
			if (mqConsumer.consumeMode().equals("CONCURRENTLY")) {
				consumer.registerMessageListener(new MessageListenerConcurrently() {

					@Override
					public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
							ConsumeConcurrentlyContext context) {
						return abstractMQPushConsumer.dealMessage(msgs, context);
					}
				});
			} else if (mqConsumer.consumeMode().equals("ORDERLY")) {
				consumer.registerMessageListener(new MessageListenerOrderly() {

					@Override
					public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
						return abstractMQPushConsumer.dealMessage(msgs, context);
					}
				});
			} else {
				throw new RuntimeException("unknown consume mode ! only support CONCURRENTLY and ORDERLY");
			}
			abstractMQPushConsumer.setConsumer(consumer);
			consumer.start();
		} else if (AbstractMQPullConsumer.class.isAssignableFrom(bean.getClass())) {

			// 配置pull consumer

			AbstractMQPullConsumer abstractMQPullConsumer = AbstractMQPullConsumer.class.cast(bean);

			DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
			consumer.setNamesrvAddr(mqProperties.getNameServerAddress());
			consumer.setMessageModel(MessageModel.valueOf(mqConsumer.messageMode()));
			consumer.setInstanceName(UUID.randomUUID().toString());
			consumer.start();

			abstractMQPullConsumer.setTopic(topic);
			abstractMQPullConsumer.setConsumer(consumer);
			abstractMQPullConsumer.startInner();
		}

		log.info(String.format("%s is ready to subscribe message", bean.getClass().getName()));
	}
}