package com.kafka.demo.controller;

import com.kafka.demo.factory.KafukaFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @desc: -.
 * @Author: lipei
 * @CreateDate: 2019/7/19 17:28
 * @Version: 1.0
 */
@RestController
public class Controller {
    private final  static KafkaProducer<String, String> kafkaProducer=KafukaFactory.MakeSimpleKafkaProducer();
    private final static String TOPIC = "logs";

    @GetMapping(value = "/pullMessage")
    public String pullMessage(String sb){
        kafkaProducer.send(new ProducerRecord<String, String>(TOPIC,"AAA",sb));
        return sb;
    }
}
