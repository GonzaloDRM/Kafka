package com.kafka.test.assingandseek;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class AssignAndSeekProducer {

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i < 100; i++){
                producer.send(new ProducerRecord<String, String>("gonza-topic", "Key", String.valueOf(i)));
            }
            producer.flush();
        }

        log.info("Processing time " + (System.currentTimeMillis() - startTime));
    }
}
