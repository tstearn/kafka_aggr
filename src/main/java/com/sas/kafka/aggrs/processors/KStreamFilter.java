package com.sas.kafka.aggrs.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamFilter<K, V> implements ProcessorSupplier<K, V> {

    private final Predicate<K, V> predicate;
    private final boolean filterNot;

    public  KStreamFilter(Predicate<K, V> predicate, boolean filterNot) {
        this.predicate = predicate;
        this.filterNot = filterNot;
    }

    @Override
    public  Processor<K, V> get() {
        return new KStreamFilterProcessor();
    }

    private class KStreamFilterProcessor extends AbstractProcessor<K, V> {
        @Override
        public  void process(K key, V value) {
            if (filterNot ^ predicate.test(key, value)) {
                context().forward(key, value);
            }
        }
    }
}
