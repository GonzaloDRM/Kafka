package com.kafka.test.assingandseek;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class AssignAndSeekConsumer {

    public static void main(String[] args){
        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "gonza-group");
        props.setProperty("enable-auto-commit", "true");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("auto.commit.intervals.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
            TopicPartition topicPartition = new TopicPartition("gonza-topic", 4);
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, 50);
            while (true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords){
                    log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }

    }
}
