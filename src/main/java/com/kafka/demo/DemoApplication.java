package com.kafka.demo;

import com.kafka.demo.factory.KafukaFactory;
import com.kafka.demo.factory.SimpleKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

import static java.lang.Thread.*;

@SpringBootApplication
public class DemoApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        KafkaConsumer<String, String> stringStringKafkaConsumer = KafukaFactory.MakeSimpleKafkaConsumer("1");
        KafkaConsumer<String, String> stringStringKafkaConsumer2 = KafukaFactory.MakeSimpleKafkaConsumer("2");
        Runnable runnable1 = () -> consume(stringStringKafkaConsumer);
        new Thread(runnable1).start();
        Runnable runnable2 = () -> {
            try {
                consume2(stringStringKafkaConsumer2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable2).start();


    }

    public void consume(KafkaConsumer<String, String> consumer){
        consumer.subscribe(Arrays.asList(SimpleKafkaConsumer.TOPIC));
        System.out.println("1号消费开始执行任务");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("1号消费"+"offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
            }
        }
    }

    public void consume2(KafkaConsumer<String, String> consumer) throws InterruptedException {
        consumer.subscribe(Arrays.asList(SimpleKafkaConsumer.TOPIC));
        System.out.println("2号消费开始执行任务");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("2号消费"+"offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
            }
            sleep(9999);
        }
    }



}
