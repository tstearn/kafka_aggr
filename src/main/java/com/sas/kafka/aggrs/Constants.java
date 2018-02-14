package com.sas.kafka.aggrs;

public class Constants {
    public static final String KAFKA_BROKER;// = "10.240.20.233:9092";
    public static final String SCHEMA_REGISTRY_URL;// = "http://10.240.20.233:8081";
    public static final long ONE_DAY_MILLIS = 24 * 60 * 60 * 1000;

    static {
        String kafkaHost = System.getProperty("KAFKA_HOST", "pass1465.na.sas.com");
        String schemaRegistryHost = System.getProperty("SCHEMA_REGISTRY_HOST", kafkaHost);

        KAFKA_BROKER = kafkaHost + ":9092";
        SCHEMA_REGISTRY_URL = "http://" + schemaRegistryHost + ":8081";
    }

    private Constants() {
    }

}
