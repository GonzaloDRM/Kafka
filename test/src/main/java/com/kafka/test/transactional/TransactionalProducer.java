package com.kafka.test.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class TransactionalProducer {

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all"); // para transacciones si o si va en all
        props.put("transactional.id", "transactional-producer-id");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            try {
                for (int i = 0; i < 1000000; i++){
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<String, String>("gonza-topic", String.valueOf(i), "mensaje desde java"));
                }
                producer.commitTransaction();
                producer.flush();
            }catch (Exception e){
                log.error("Error", e);
                producer.abortTransaction();
            }

        }

        log.info("Processing time " + (System.currentTimeMillis() - startTime));
    }
}
