package com.sas.kafka.aggrs.project;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.List;

public class AggregateGroup {
    private String filter;
    private LookbackUnit lookbackUnit;
    private Long lookbackPeriod;
    private List<Aggregation> aggregates;
    private String storeName;

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Predicate<String,JsonNode> getFilterAsPredicate() {
        //For right now, just hardcoding predicate.  In future, would derive it from filter, which will likely be a structured
        //object rather than a string
        Predicate<String,JsonNode> filterPred = (key, value)->
                value.get("accountTypeDesc").asText().equals("P") &&
                        value.get("primaryMediumDesc").asText().equals("CASH");

        return filterPred;
    }

    public Long getLookbackPeriod() {
        return lookbackPeriod;
    }

    public void setLookbackPeriod(Long lookbackPeriod) {
        this.lookbackPeriod = lookbackPeriod;
    }

    public LookbackUnit getLookbackUnit() {
        return lookbackUnit;
    }

    public void setLookbackUnit(LookbackUnit lookbackUnit) {
        this.lookbackUnit = lookbackUnit;
    }

    public List<Aggregation> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<Aggregation> aggregates) {
        this.aggregates = aggregates;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }
}
