package com.sas.kafka.aggrs

import com.sas.kafka.aggrs.engine.ProjectExecutor
import com.sas.kafka.aggrs.project.AggregateGroup
import com.sas.kafka.aggrs.project.Aggregation
import com.sas.kafka.aggrs.project.AggregationFunction
import com.sas.kafka.aggrs.project.LookbackUnit
import com.sas.kafka.aggrs.project.Project

class RunAggregationEngine {
    static void main(String[] args) throws Exception {
        Project project = buildProject()
        ProjectExecutor executor = new ProjectExecutor(project)
        executor.run()

        //Sleep for a while to allow engine to process data
        Thread.sleep(10000)

        executor.stop()
    }

    private static Project buildProject() {
        return new Project(
                appId: "kafkaAggrEngine",
                inputTopicName: "transactions10kJson",
                outputTopicName: "transactions10kJson-enhanced",
                partitionKeyColumnName: "partyNumber",
                aggregateGroups: [
                        new AggregateGroup(
                                filter: "",
                                lookbackUnit: LookbackUnit.DAY,
                                lookbackPeriod: 2,
                                storeName: "group1",
                                aggregates: [
                                      new Aggregation(
                                              inputFieldName: "currencyAmount",
                                              outputFieldName: "sum2d",
                                              aggregationFunction: AggregationFunction.SUM
                                      ),
                                      new Aggregation(
                                              inputFieldName: "currencyAmount",
                                              outputFieldName: "count2d",
                                              aggregationFunction: AggregationFunction.COUNT
                                      )
                                ]
                        )

                ]
        )
    }
}
