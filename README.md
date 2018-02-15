# kafka_aggr

This project prototypes real-time aggregations using Kafka.  The motivation is to overcome the memory/performance constraints of ESP to handle common fraud use cases.

Kafka take an approach similar to the "state vector" approach of Raptor.  Aggregates are written to a "store" and store entries are retrieved an updated as records flow through the system.  The overall approach is to
* Read original event from a Kafka topic
* Update aggregate values on "state vector" store
* Append all aggregate fields to original event, creating an enhanced event
* Write the enhanced event to a Kafka output topic, which can then be read by ESP and process further

## Development server

While Kafka is available as an Apache open source project, most implementations use the Confluent implementation.  Among other things, Confluent provides a Schema Repository for use when reading/writing Avro data in Kafka.  The Confluent implementation only runs on Linux.  The following links will be helpful:
* [Confluent Installation](https://docs.confluent.io/3.2.2/installation/installing_cp.html)
* [Confluent Quick Start](https://docs.confluent.io/3.2.2/quickstart.html)

This code was tested with Confluent 3.2.2.

## Using Kafka at the command line
* Start environment:  Run the commands in the "scripts/kafka_env_startup.sh" script
* Before running tests, clear and recreate the topics using the commands in the "scripts/clear_create_topics.sh" script
* To view topics with JSON content, run:  bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic name> --from-beginning

## Note on data format

The most efficient format to use with Kafka is Avro, since this is a binary format that does not need to be reparsed each time it is read.  The prototype code uses this format for intermediate "store" format.  To use Avro, you need to
1. Create schema definition files (the .avsc files in the src/main/acvro directory)
2. Compile the schema files with the "generateAvroJava" gradle task

At the time of this writing, ESP does not support reading Avro.  Plus, some research would be required to determine how we could support the Avro format for client input data.  So, for now the prototype reads/writes JSON to and from public topics.

## Running demo
1.  Use the commands documented above to recreate the topics
2.  Run the LoadTransactions10KtoKafkaJson program to load up the input topic
3.  Run the RunAggregationEngine program to read the input topic and create records on the output topic

Note that the load program depends on the fraud dev image database being available for query.  Also, both programs and the Contants class refer to pass1465 at the time of this writing for the Kafka execution environment.  This can be changed to point elsewhere by editing the code.

## Design details
The basic design is for a micro-service that will read metadata describing the require aggregations, along with the source and target topics and generate the proper Kafka pipeline from this metadata.  All the metadata is currently housed under the Project object. A project is used to initialize a ProjectExecutor and the run() method is called on the ProjectExecutor to begin monitoring the topic and outputting events. Possibly each ProjectExecutor would exist in it's own thread and the controlling micro-service could accept request to start/stop Projects.

Contrary to initial thoughts, not Java code generation/compilation should be required for the solution 

## Areas for improvement
1.  Has only been tested with tiny amounts of data - needs more robust testing in terms of volume and variety
2.  Filters for AggregateGroup are currently hard code
    1. Will need to model the filter as domain object 
    2. Update the getFilterAsPredicate() method to do dynamic filtering using the metadata and JsonNode
3.  All aggregate "state vector" definitions currently calculate all aggregate functions
    1.  This could be optimized, but some aggregate functions (avg, stddev) need additional supporting attributes for incremental calculation
4.  The use of lookbackUnit has not been totally generalized.  In at least one place, DAY is hard coded.    
    

## Open Questions
1.  Need to confirm that the AggregateGroupStoresUpdater.apply() and AggregateAppender.apply() functionality will always run serially, with the first feeding the second
    1.  This appears to be the case with testing this far, but testing has been far from extensive
    2.  If this is NOT the case, it BREAKS THE WHOLE CONCEPT!