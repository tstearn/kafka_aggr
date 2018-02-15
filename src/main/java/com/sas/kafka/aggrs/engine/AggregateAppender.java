package com.sas.kafka.aggrs.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sas.kafka.aggrs.domain.AdditiveStatistics;
import com.sas.kafka.aggrs.domain.AggregateGroupValue;
import com.sas.kafka.aggrs.project.AggregateGroup;
import com.sas.kafka.aggrs.project.Aggregation;
import com.sas.kafka.aggrs.project.Project;
import com.sas.kafka.aggrs.serialization.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AggregateAppender {
    private Project project;
    private KStream<String, JsonNode> events;
    private KafkaStreams streams;
    private Properties streamsConfig;

    public AggregateAppender(Project project, KStream<String, JsonNode> events, Properties streamsConfig) {
        this.project = project;
        this.events = events;
        this.streamsConfig = streamsConfig;
    }

    public void setStreams(KafkaStreams streams) {
        this.streams = streams;
    }

    public void apply() {
        JsonSerde<JsonNode> outSerde = new JsonSerde<>(streamsConfig);
        String[] requiredStores = project.getAggregateGroups().stream()
                .map(aggregateGroup -> aggregateGroup.getStoreName())
                .toArray(String[]::new);
        //Need to specify required stores so that Kafka knows the dependencies and create the pipeline in the right order
        events.transformValues(new AggregateStoreLookupProcessorSupplier(),requiredStores)
                .to(Serdes.String(),outSerde,project.getOutputTopicName());
    }

    private class AggregateStoreLookupProcessorSupplier implements ValueTransformerSupplier<JsonNode,JsonNode> {

        @Override
        public ValueTransformer<JsonNode, JsonNode> get() {
            return new AggregateStoreLookupProcessor();
        }

        private class AggregateStoreLookupProcessor implements ValueTransformer<JsonNode,JsonNode> {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public JsonNode transform(JsonNode inputEvent) {
                JsonNode outputEvent = inputEvent.deepCopy();
                //For each aggregate group, retrieve the store and append the aggregate variables
                for(AggregateGroup aggregateGroup : project.getAggregateGroups()) {
                    //If filter applies to record, retrieve aggregate vaiables and append them to event
                    if(aggregateGroup.getFilterAsPredicate().test(null,inputEvent)) {
                        AggregateGroupValue groupAggrVals = getAggregateValueForStoreAndEvent(aggregateGroup.getStoreName(),inputEvent);
                        if(groupAggrVals != null) {
                            addAggregatesToEvent(aggregateGroup.getAggregates(),groupAggrVals,(ObjectNode)outputEvent);
                        }
                    }
                }

                return  outputEvent;
            }

            private AggregateGroupValue getAggregateValueForStoreAndEvent(String storeName, JsonNode event) {
                ReadOnlyWindowStore<String, AggregateGroupValue> windowStore = streams.store(storeName, QueryableStoreTypes.windowStore());
                long eventDate = event.get(Project.EVENT_DATE_FIELD).asLong();
                long timeFrom = eventDate - TimeUnit.DAYS.toMillis(1);
                long timeTo = eventDate + 1;

                WindowStoreIterator<AggregateGroupValue> iterator = windowStore.fetch(event.get(project.getPartitionKeyColumnName()).asText(), timeFrom,timeTo);
                if(iterator.hasNext()) {
                    KeyValue<Long, AggregateGroupValue> windowKv = iterator.next();
                    return windowKv.value;
                }
                return  null;
            }

            private void addAggregatesToEvent(List<Aggregation> aggrDefs, AggregateGroupValue aggregateGroupValues, ObjectNode event) {
                for(Aggregation aggr : aggrDefs) {
                    AdditiveStatistics aggrVal = aggregateGroupValues.getEntries().get(aggr.getOutputFieldName());
                    event.put(aggr.getOutputFieldName(),getStatForAggregation(aggr,aggrVal));
                }
            }

            private Double getStatForAggregation(Aggregation aggr, AdditiveStatistics aggrValue) {
                switch(aggr.getAggregationFunction()) {
                    case COUNT:
                        return aggrValue.getCount();
                    case SUM:
                        return aggrValue.getSum();
                    case MIN:
                        return aggrValue.getMin();
                    case MAX:
                        return aggrValue.getMax();
                    case MEAN:
                        return aggrValue.getAverage();
                    case VAR:
                        return aggrValue.getVariance();
                    case STDDEV:
                        return aggrValue.getStddev();
                }
                return null;
            }

            @Override
            public JsonNode punctuate(long timestamp) {
                return null;
            }

            @Override
            public void close() {

            }
        }

    }
}
