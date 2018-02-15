package com.sas.kafka.aggrs.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.sas.kafka.aggrs.Constants;
import com.sas.kafka.aggrs.project.Project;
import com.sas.kafka.aggrs.serialization.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class ProjectExecutor implements Runnable{
    private Project project;
    private Properties streamsConfig;
    private KafkaStreams streams;
    private AggregateGroupStoresUpdater storesUpdater;
    private AggregateAppender appender;

    public ProjectExecutor(Project project) {
        this.project = project;
    }

    public void run() {
        configStream();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,JsonNode> events = builder.stream(project.getInputTopicName());

        buildPipeline(events);

        streams = new KafkaStreams(builder, streamsConfig);
        appender.setStreams(streams);

        streams.cleanUp();
        streams.start();
    }

    public void stop() {
        System.out.println(streams.toString());
        streams.close();
    }

    private void configStream() {
        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, project.getAppId());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
        streamsConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfig.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeStampExtractor.class);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);
        streamsConfig.put("specific.avro.reader", true);
        streamsConfig.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private void buildPipeline(KStream<String,JsonNode> events) {
        //Update aggregate stores with incoming records
        storesUpdater = new AggregateGroupStoresUpdater(project.getAggregateGroups(),events,streamsConfig);
        storesUpdater.apply();
        //Append updated aggregate values (from stores) to incoming records
        appender = new AggregateAppender(project,events,streamsConfig);
        appender.apply();
    }
}
