package com.sranjan001.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sranjan001.kafka.domain.Item;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemSerializer implements Serializer<Item> {
    private Logger logger = LoggerFactory.getLogger(ItemSerializer.class);
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Item item) {
        logger.info("Inside serialize method");
        try {
            return objectMapper.writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.info("JsonProcessingException in serializable {}", item,e);
            logger.info("JsonProcessingException in serializable {}", item,e);
            logger.info("JsonProcessingException in serializable {}", item,e);
            logger.info("JsonProcessingException in serializable {}", item,e);
            logger.info("JsonProcessingException in serializable {}", item,e);
            return null;
        }
    }
}
