package com.sas.kafka.aggrs.serialization;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Map;
import java.util.Properties;

public class JsonSerde<JsonNode> implements Serde<JsonNode> {
    private final Serializer serializer = new JsonSerializer();
    private final Deserializer deserializer = new JsonDeserializer();

    public JsonSerde() {
    }

    public JsonSerde(Properties properties){
        configure((Map)properties, false);
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public Deserializer getDeserializer() {
        return deserializer;
    }

    @Override
    public final void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<JsonNode> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return deserializer;
    }

}
