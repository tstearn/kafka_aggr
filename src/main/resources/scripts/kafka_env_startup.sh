#!/usr/bin/env bash
#Run from the confluent directory

nohup ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties > zookeeper.log &
nohup bin/kafka-server-start ./etc/kafka/server.properties > kafkaServer.log &
nohup bin/schema-registry-start ./etc/schema-registry/schema-registry.properties  &

