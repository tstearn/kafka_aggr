package com.sas.kafka.aggrs.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.sas.kafka.aggrs.aggregators.AggregateGroupValueUtil;
import com.sas.kafka.aggrs.domain.AggregateGroupValue;
import com.sas.kafka.aggrs.project.AggregateGroup;
import com.sas.kafka.aggrs.serialization.AvroSerde;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AggregateGroupStoresUpdater {
    private List<AggregateGroup> aggregateGroups;
    private KStream<String,JsonNode> events;
    private Properties streamsConfig;

    public AggregateGroupStoresUpdater(List<AggregateGroup> aggregateGroups, KStream<String, JsonNode> events, Properties streamsConfig) {
        this.aggregateGroups = aggregateGroups;
        this.events = events;
        this.streamsConfig = streamsConfig;
    }

    public void apply() {
        for(AggregateGroup aggregateGroup : aggregateGroups) {
            KGroupedStream<String,JsonNode> filteredGroupedStream = events.filter(aggregateGroup.getFilterAsPredicate())
                    .groupByKey();
            updateAggregateGroupStore(aggregateGroup,filteredGroupedStream);
        }
    }

    private KStream<String,JsonNode> applyFilter() {
        Predicate<String,JsonNode> filterPred = (key, value)->
                value.get("accountTypeDesc").asText().equals("P") &&
                value.get("primaryMediumDesc").asText().equals("CASH");

        return events.filter(filterPred);
    }

    private void updateAggregateGroupStore(AggregateGroup aggregateGroup, KGroupedStream<String,JsonNode> filteredRecords) {
        TimeUnit lookbackUnit = getTimeUnit(aggregateGroup);
        TimeWindows windowDef = TimeWindows.of(lookbackUnit.toMillis(aggregateGroup.getLookbackPeriod())).advanceBy(lookbackUnit.toMillis(1));

        filteredRecords.aggregate(AggregateGroupValue::new,
                (key,value,accumulator)-> AggregateGroupValueUtil.advance(aggregateGroup,value,accumulator),
                windowDef,
                new AvroSerde<AggregateGroupValue>(streamsConfig),
                aggregateGroup.getStoreName());

    }

    private TimeUnit getTimeUnit(AggregateGroup aggregateGroup) {
        switch(aggregateGroup.getLookbackUnit()) {
            case DAY:
                return TimeUnit.DAYS;
            case HOUR:
                return TimeUnit.HOURS;
            case MINUTE:
                return TimeUnit.MINUTES;
            case SECOND:
                return TimeUnit.SECONDS;
            default:
                return null;
        }
    }
}
