package com.sas.kafka.aggrs.domain;


import com.sas.kafkaaggr.domain.Transactions10K;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class Transactions10TimestampExtractor implements TimestampExtractor{
    public Transactions10TimestampExtractor() {
        int x = 0;
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        long timestamp = -1;

        if(record.value() instanceof Transactions10K) {
            Transactions10K myPojo = (Transactions10K)record.value();
            if (myPojo != null) {
                timestamp = myPojo.getTransDate();
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
