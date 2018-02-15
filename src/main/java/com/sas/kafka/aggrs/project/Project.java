package com.sas.kafka.aggrs.project;

import java.util.List;

public class Project {
    public final static String EVENT_DATE_FIELD = "__eventDate";
    String appId;
    String inputTopicName;
    String outputTopicName;
    String partitionKeyColumnName;
    List<AggregateGroup> aggregateGroups;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getInputTopicName() {
        return inputTopicName;
    }

    public void setInputTopicName(String inputTopicName) {
        this.inputTopicName = inputTopicName;
    }

    public String getOutputTopicName() {
        return outputTopicName;
    }

    public void setOutputTopicName(String outputTopicName) {
        this.outputTopicName = outputTopicName;
    }

    public String getPartitionKeyColumnName() {
        return partitionKeyColumnName;
    }

    public void setPartitionKeyColumnName(String partitionKeyColumnName) {
        this.partitionKeyColumnName = partitionKeyColumnName;
    }

    public List<AggregateGroup> getAggregateGroups() {
        return aggregateGroups;
    }

    public void setAggregateGroups(List<AggregateGroup> aggregateGroups) {
        this.aggregateGroups = aggregateGroups;
    }
}
