package com.sranjan001.kafka.consumer;

import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumerASynchronousCommit {

    private final static Logger logger = LoggerFactory.getLogger(MessageConsumerASynchronousCommit.class);

    KafkaConsumer<String, String> kafkaConsumer;
    String topic = "test-topic-replicated";

    public MessageConsumerASynchronousCommit(Map<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.96:9092, 192.168.0.96:9093, 192.168.0.96:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer");
//        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
//        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return propsMap;
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(List.of(topic));

        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    logger.info("Consumer Record key is {} and the value is {} and the partition {}", record.key(), record.value(), record.partition());
                });

                if (consumerRecords.count() > 0) {
                   // kafkaConsumer.commitAsync(); //the last record offset return by the poll call.
                    kafkaConsumer.commitAsync((offset, exception) -> {
                        if(exception != null)
                            logger.error("Error committing the offset {}", exception.getMessage());
                        else
                            logger.info("Offset committed !!!");

                    });
                }
            }
        } catch (CommitFailedException e) {
            logger.error("Commit Failed exception: " + e);
        }  catch (Exception e) {
            logger.error("Exception in pollKafka : " + e);
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumerASynchronousCommit messageConsumer = new MessageConsumerASynchronousCommit(buildConsumerProperties());
        messageConsumer.pollKafka();
    }
}
