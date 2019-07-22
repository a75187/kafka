package com.kafka.demo.factory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @desc: -.
 * @Author: lipei
 * @CreateDate: 2019/7/19 15:02
 * @Version: 1.0
 */

public class SimpleKafkaProducer {
    private static KafkaProducer<String, String> producer;
    private final static String TOPIC = "logs";
    public  static KafkaProducer<String, String> MakeSimpleKafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.62.139:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
       return producer = new KafkaProducer<String, String>(props);
    }
    public void produce(){
        for (int i = 1;i<10;i++){
            String key = String.valueOf(i);
            String data = "你今天第"+key+"次吃饭";
            producer.send(new ProducerRecord<String, String>(TOPIC,key,data));
            System.out.println(data);
        }

    }

    public static void main(String[] args) {
        new SimpleKafkaProducer().produce();
    }
}
