package com.sas.kafka.aggrs.project;

public class Aggregation {
    private String inputFieldName;
    private String outputFieldName;
    private AggregationFunction aggregationFunction;

    public String getInputFieldName() {
        return inputFieldName;
    }

    public void setInputFieldName(String inputFieldName) {
        this.inputFieldName = inputFieldName;
    }

    public String getOutputFieldName() {
        return outputFieldName;
    }

    public void setOutputFieldName(String outputFieldName) {
        this.outputFieldName = outputFieldName;
    }

    public AggregationFunction getAggregationFunction() {
        return aggregationFunction;
    }

    public void setAggregationFunction(AggregationFunction aggregationFunction) {
        this.aggregationFunction = aggregationFunction;
    }
}
