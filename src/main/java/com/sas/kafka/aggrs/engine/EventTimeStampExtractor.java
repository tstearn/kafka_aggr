package com.sas.kafka.aggrs.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.sas.kafka.aggrs.project.Project;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EventTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        long timestamp = -1;

        if(record.value() instanceof JsonNode) {
            JsonNode event = (JsonNode) record.value();
            if (event != null) {
                timestamp = event.get(Project.EVENT_DATE_FIELD).asLong();
            }
        }
        else {
            timestamp = record.timestamp();
        }

        if (timestamp < 0) {
            // Invalid timestamp!  Attempt to estimate a new timestamp,
            // otherwise fall back to wall-clock time (processing-time).
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }

        return timestamp;
    }
}
