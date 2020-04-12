package com.sranjan001.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {
    private final static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private String topic = "test-topic-replicated";
    private KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    Callback callback = (recordMetadata, exception) -> {
        if(exception != null) {
            logger.error("Exception in callback {}", exception.getMessage());
        } else {
            logger.info("Published Message Offset in callback is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
            logger.info("partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        }
    };

    public static Map<String, Object> propsMap() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.96:9092,192.168.0.96:9093,192.168.0.96:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return propsMap;
    }

    public void publishMessageSync(String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent succssfully for key {}", value, key);
            logger.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
            logger.info("partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("Exception in publishMessageSync {}", e.getMessage());
        }
    }

    public void publishMessageAsync(String key, String value) throws InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

        kafkaProducer.send(producerRecord, callback);
        Thread.sleep(3000);
    }

    public void close() {
        kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        MessageProducer messageProducer = new MessageProducer(propsMap());
        messageProducer.publishMessageSync("99", "ABC");
        messageProducer.publishMessageSync("99", "DEF");
//        messageProducer.publishMessageAsync(null, "ABC-ASYNC");
    }

}
