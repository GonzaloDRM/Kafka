package com.kafka.test.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class TestProducer {

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // broker de kafka al que nos vamos a conectar
        props.put("acks", "all"); // acknowledge tipo de coneccion puede ser 0,1,all
                                // que acknowledge este en 0 quiere decir que no importa si se recibe o no, en 1
                                // quiere decir que un nodo al menos tiene que avisar que se recibio y si pones all
                                // todos los nodos tienen que confirmar que se recibio
        //props.put("transactional.id", "gonza-producer-id");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Como se envia la informacion
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Puede ser String, Long, Short, etc serializer
        props.put("linger.ms", "10"); // aca se configura la cantidad de agrupacion de batchers, tambien se puede configurar con batch.size pero este es mas rapido

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i < 100000; i++){
                producer.send(new ProducerRecord<String, String>("gonza-topic", String.valueOf(i), "mensaje desde java")); // esta todo asincrono, si le agrego un .get al final se vuelve sincrono
            }
            producer.flush(); // esto es para que envie todo los mensajes pendientes y no deje nada en el camino
        }

        log.info("Processing time " + (System.currentTimeMillis() - startTime));
    }


/*
    Esto es sincrono
    try (Producer<String, String> producer = new KafkaProducer<>(props)){
        for (int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>("gonza-topic", String.valueOf(i), "mensaje desde java")).get();
        }
        producer.flush(); // esto es para que envie todo los mensajes pendientes y no deje nada en el camino
    } catch (InterruptedException | ExecutionException e){
        log.error("Message producer interrupted " + e);
    }
*/

}
