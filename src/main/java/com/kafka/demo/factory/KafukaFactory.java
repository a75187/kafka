package com.kafka.demo.factory;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * @desc: -.
 * @Author: lipei
 * @CreateDate: 2019/7/19 17:14
 * @Version: 1.0
 */

public class KafukaFactory {
    public static KafkaConsumer<String, String> MakeSimpleKafkaConsumer(String id){
        KafkaConsumer<String, String> stringStringKafkaConsumer = SimpleKafkaConsumer.makeSimpleKafkaConsumer(id);
        return stringStringKafkaConsumer;
    }


    public static KafkaProducer<String, String>   MakeSimpleKafkaProducer() {
        KafkaProducer<String, String> stringStringKafkaProducer = SimpleKafkaProducer.MakeSimpleKafkaProducer();
        return stringStringKafkaProducer;
    }
}
