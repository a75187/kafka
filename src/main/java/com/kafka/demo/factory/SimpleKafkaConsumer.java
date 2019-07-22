package com.kafka.demo.factory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @desc: -.
 * @Author: lipei
 * @CreateDate: 2019/7/19 15:03
 * @Version: 1.0
 */

public class SimpleKafkaConsumer {

   // private static KafkaConsumer<String, String> consumer;
    public final static String TOPIC = "logs";
    public static  KafkaConsumer<String, String> makeSimpleKafkaConsumer(String a1){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.62.139:9092");
        //每个消费者分配独立的组号
        props.put("group.id", a1);
        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");
        //设置多久一次更新被消费消息的偏移量
        props.put("auto.commit.interval.ms", "1000");
        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("session.timeout.ms", "30000");
        //自动重置offset
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
       return  new KafkaConsumer<String, String>(props);
    }





}
