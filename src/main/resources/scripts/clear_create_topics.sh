#!/usr/bin/env bash

bin/kafka-topics --delete --zookeeper localhost:2181 --topic transactions10kJson
bin/kafka-topics --delete --zookeeper localhost:2181 --topic transactions10kJson-enhanced
bin/kafka-topics --delete --zookeeper localhost:2181 --topic kafkaAggrEngine-group1-changelog


bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic transactions10kJson
bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic transactions10kJson-enhanced
