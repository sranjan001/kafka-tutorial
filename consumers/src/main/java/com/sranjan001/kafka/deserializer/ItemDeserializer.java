package com.sranjan001.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sranjan001.kafka.domain.Item;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ItemDeserializer implements Deserializer<Item> {
    private static final Logger logger = LoggerFactory.getLogger(ItemDeserializer.class);

    @Override
    public Item deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, Item.class);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error in deserialize", e);
            return null;
        }
    }
}
