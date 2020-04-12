package com.sranjan001.kafka.consumer;

import com.sranjan001.kafka.deserializer.ItemDeserializer;
import com.sranjan001.kafka.domain.Item;
import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class ItemConsumer {

    private final static Logger logger = LoggerFactory.getLogger(ItemConsumer.class);

    KafkaConsumer<Integer, Item> kafkaConsumer;
    String topic = "items";

    public ItemConsumer(Map<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<Integer, Item>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.96:9092, 192.168.0.96:9093, 192.168.0.96:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ItemDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "itemsGroupId");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
//        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");

        return propsMap;
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(topic));

        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);

        try {
            while(true) {
                ConsumerRecords<Integer, Item> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    logger.info("Consumer Record key is {} and the value is {} and the partition {}", record.key(), record.value(), record.partition());
                });
            }
        } catch (Exception e) {
            logger.error("Exception in pollKafka : " + e);
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        ItemConsumer messageConsumer = new ItemConsumer(buildConsumerProperties());
        messageConsumer.pollKafka();
    }
}
