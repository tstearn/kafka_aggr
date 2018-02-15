package com.sas.kafka.aggrs.aggregators;


import com.sas.kafka.aggrs.domain.AdditiveStatistics;
import com.sas.kafka.aggrs.domain.AggregateGroupValue;
import com.sas.kafka.aggrs.project.AggregateGroup;
import com.sas.kafka.aggrs.project.Aggregation;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;

public class AggregateGroupValueUtil {
    public static AggregateGroupValue advance(AggregateGroup aggregateGroup, JsonNode record, AggregateGroupValue accumulator) {
        for(Aggregation aggr : aggregateGroup.getAggregates()) {
            if(accumulator.getEntries() == null) {
                accumulator.setEntries(new HashMap<>());
            }

            String entryName = aggr.getOutputFieldName();
            AdditiveStatistics stats = accumulator.getEntries().get(entryName);
            if(stats == null) {
                stats = new AdditiveStatistics();
                accumulator.getEntries().put(entryName,stats);
            }

            AdditiveStatsUtil.advance(record.get(aggr.getInputFieldName()).asDouble(),stats);
        }
        return accumulator;
    }
}

