package com.sranjan001.kafka.consumer;

import com.sranjan001.kafka.consumer.listener.MessageRebalanceListener;
import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumerSeek {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerSeek.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic-replicated";
    public static final String serialiaziedFilePath = "consumers/src/main/resources/offset.ser";

    public MessageConsumerSeek(Map<String, Object> propsMap) {
        kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
    }

    public static Map<String, Object> buildConsumerProperties() {

        Map<String, Object> propsMap = new HashMap<>();

        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.96:9092,192.168.0.96:9093,192.168.0.96:9094");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconsumer");
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return propsMap;
    }

    public void pollKafka() throws IOException, ClassNotFoundException {

        kafkaConsumer.subscribe(List.of(topicName), new MessageRebalanceListener(kafkaConsumer));
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach((record) -> {
                    logger.info("Consumer Record Key is {} and the value is {} and the partion {} and the offset is {}",
                            record.key(), record.value(), record.partition(), record.offset());
                    offsetsMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, null));
                    //The committed offset should always be the offset of the next message
                });
                if(consumerRecords.count()>0){
                    logger.info("offsetsMap : {} ", offsetsMap);
                    //kafkaConsumer.commitSync(offsetsMap); // commits the last record offset read by the poll invocation
                    writeOffsetsMapToPath(offsetsMap);
                }
            }
        } catch (CommitFailedException e) {
            logger.error("CommitFailedException in pollKafka : " + e);
        } catch (Exception e) {
            logger.error("Exception in pollKafka : " + e);
        } finally {
            kafkaConsumer.close();
        }

    }
    private void writeOffsetsMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetsMap) throws IOException {

        FileOutputStream fout = null;
        ObjectOutputStream oos = null;
        try {
            fout = new FileOutputStream(serialiaziedFilePath);
            oos = new ObjectOutputStream(fout);
            oos.writeObject(offsetsMap);
            logger.info("Offsets Written Successfully!");
        } catch (Exception ex) {
            logger.error("Exception Occurred while writing the file : " + ex);
        } finally {
            if(fout!=null)
                fout.close();
            if(oos!=null)
                oos.close();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        MessageConsumerSeek messageConsumer = new MessageConsumerSeek(buildConsumerProperties());
        messageConsumer.pollKafka();
    }
}
