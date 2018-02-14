package com.sas.kafka.aggrs.serialization;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;

public class AvroSerde<T> implements Serde<T> {
    private final Serializer serializer = new KafkaAvroSerializer();
    private final Deserializer deserializer = new KafkaAvroDeserializer();

    public AvroSerde() {
    }

    public AvroSerde(Properties properties){
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
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

}
