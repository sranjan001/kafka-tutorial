package com.sranjan001.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sranjan001.kafka.domain.Item;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemProducerApproach2 {
    private final static Logger logger = LoggerFactory.getLogger(ItemProducerApproach2.class);

    private String topic = "items";
    private KafkaProducer<Integer, String> kafkaProducer;
    private ObjectMapper objectMapper = new ObjectMapper();

    public ItemProducerApproach2(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<Integer, String>(propsMap);
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
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return propsMap;
    }

    public void publishMessageSync(Item item) {
        try {
            String value = objectMapper.writeValueAsString(item);
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord(topic, item.getId(), value);
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent succssfully for key {}", item.getId(), value);
            logger.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
            logger.info("partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in publishMessageSync {}", e.getMessage());
        }
    }


    public void close() {
        kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        ItemProducerApproach2 messageProducer = new ItemProducerApproach2(propsMap());
        Item item1 = new Item(1, "LG TV", 400.0);
        Item item2 = new Item(2, "Iphone max", 949.0);

        List.of(item1, item2).forEach(item -> {
            messageProducer.publishMessageSync(item);
        });
    }

}
