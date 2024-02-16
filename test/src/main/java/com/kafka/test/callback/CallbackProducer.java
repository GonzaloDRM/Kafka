package com.kafka.test.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

@Slf4j
public class CallbackProducer {

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i < 10000; i++){
                producer.send(new ProducerRecord<String, String>("gonza-topic", String.valueOf(i), "mensaje desde java"), (metadata, exception) -> {
                    if (exception != null) {
                        log.info("There was an error {} ", exception.getMessage());
                    }
                    log.info("Offset = {}, Partition = {}, Topic = {}", metadata.offset(), metadata.partition(), metadata.topic());

                });
            }
            producer.flush(); // esto es para que envie todo los mensajes pendientes y no deje nada en el camino
        }
        log.info("Processing time " + (System.currentTimeMillis() - startTime));
    }

}

/** SIN LAMBDA
 * try (Producer<String, String> producer = new KafkaProducer<>(props)){
 *             for (int i = 0; i < 10000; i++){
 *                 producer.send(new ProducerRecord<String, String>("gonza-topic", String.valueOf(i), "mensaje desde java"), new Callback() {
 *                     @Override
 *                     public void onCompletion(RecordMetadata recordMetadata, Exception e) {
 *                         if (e != null){
 *                             log.info("There was an error {} ", e.getMessage());
 *                         }
 *                         log.info("Offset = {}, Partition = {}, Topic = {}", recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic());
 *                     }
 *                 });
 *             }
 *             producer.flush(); // esto es para que envie todo los mensajes pendientes y no deje nada en el camino
 *         }
 */
