package com.sranjan001.kafka.producer;

import com.sranjan001.kafka.domain.Item;
import com.sranjan001.kafka.serializer.ItemSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ItemProducer {
    private final static Logger logger = LoggerFactory.getLogger(ItemProducer.class);

    private String topic = "items";
    private KafkaProducer<Integer, Item> kafkaProducer;

    public ItemProducer(Map<String, Object> propsMap) {
        kafkaProducer = new KafkaProducer<Integer, Item>(propsMap);
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.96:9092,192.168.0.96:9093,192.168.0.96:9094");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ItemSerializer.class.getName());

        return propsMap;
    }

    public void publishMessageSync(Item item) {
        ProducerRecord<Integer, Item> producerRecord = new ProducerRecord(topic, item.getId(), item);
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent succssfully for key {}", item, item.getId());
            logger.info("Published Message Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
            logger.info("partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("Exception in publishMessageSync {}", e.getMessage());
        }
    }

    public void close() {
        kafkaProducer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        ItemProducer messageProducer = new ItemProducer(propsMap());
        Item item1 = new Item(1, "LG TV", 400.0);
        Item item2 = new Item(2, "Iphone max", 949.0);

        List.of(item1, item2).forEach(item -> {
            messageProducer.publishMessageSync(item);
        });

    }

}
