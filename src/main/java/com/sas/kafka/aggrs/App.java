package com.sas.kafka.aggrs;


import com.sas.kafka.aggr.domain.Transactions10KEnhanced;
import com.sas.kafka.aggrs.aggregators.AdditiveStatsUtil;
import com.sas.kafka.aggrs.domain.AdditiveStatistics;
import com.sas.kafka.aggrs.domain.Transactions10TimestampExtractor;
import com.sas.kafka.aggrs.serialization.AvroSerde;
import com.sas.kafkaaggr.domain.Transactions10K;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.KStreamWindowAggregate;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.*;
import com.sas.kafka.aggrs.processors.*;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;
import org.apache.kafka.streams.state.internals.WindowStoreSupplier;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class App {
    private static String inputTopic = "transactions10k";
    private static String outputTopic = "transactions10k-enhanced";
    private static String outputStoreName = "transactions10k-enhanced-store";
    private static String windowStoreName = "sas10005_store";
    private static KafkaStreams streams = null;

    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "vsd_aggrs");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKER);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, AvroSerde.class.getName());
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);
        props.put("specific.avro.reader", true);
        props.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, Transactions10TimestampExtractor.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();
        TimeWindows twoDayHopping = TimeWindows.of(TimeUnit.DAYS.toMillis(2)).advanceBy(TimeUnit.DAYS.toMillis(1));
        AvroSerde<AdditiveStatistics> metricsSerde = new AvroSerde<>(props);
        AvroSerde<Transactions10KEnhanced> enhancedTransSerde = new AvroSerde<>(props);

        //Create store for output records
        Map<String,String> logConfig = new HashMap<>();
        StateStoreSupplier windowStore = new RocksDBWindowStoreSupplier(windowStoreName,
                                                                        TimeUnit.DAYS.toMillis(1),
                                                                        2,
                                                                        false,
                                                                         Serdes.String(),
                                                                         metricsSerde,
                                                                         TimeUnit.DAYS.toMillis(2),
                                                                        true,
                                                                         logConfig,
                                                                        true);

        builder.addStateStore(windowStore);

        Predicate<String,Transactions10K> filterPred = (key,value)->
            value.getAccountTypeDesc().equals("P") && value.getPrimaryMediumDesc().equals("CASH");


        KStreamWindowAggregate<String,Transactions10K,AdditiveStatistics,TimeWindow> aggregator
                = new KStreamWindowAggregate<>(twoDayHopping,
                                               windowStoreName,
                                               AdditiveStatistics::new,
                                               (key,value,accumulator)-> AdditiveStatsUtil.advance(value.getCurrencyAmount(),accumulator));


        builder.addSource(TopologyBuilder.AutoOffsetReset.EARLIEST,
                "source",
                new StringDeserializer(),
                enhancedTransSerde.getDeserializer(),
                inputTopic);
        builder.addProcessor("filter",new KStreamFilter<String,Transactions10K>(filterPred,false),"source");
        builder.addProcessor("window",aggregator,"filter");
        builder.connectProcessorAndStateStores("window",windowStoreName);
        builder.addProcessor("join",WindowProcessor::new,"filter");
        builder.addSink("sink",outputTopic,"join");

        streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.start();

        // Print the internal topology to stdout
        System.out.println(streams.toString());

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(10000);

        // Print the internal topology to stdout
        System.out.println(streams.toString());

        streams.close();
    }

    static class WindowProcessor implements Processor<String, Transactions10K>  {

        private ProcessorContext context;
        private KeyValueStore<String, Transactions10KEnhanced> outStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(String dummy, Transactions10K event) {
            ReadOnlyWindowStore<String, AdditiveStatistics> windowStore = streams.store(windowStoreName, QueryableStoreTypes.windowStore());
            long timeFrom = event.getTransDate() - TimeUnit.DAYS.toMillis(1);
            long timeTo = event.getTransDate() -1;
            WindowStoreIterator<AdditiveStatistics> iterator = windowStore.fetch(event.getPartyNumber(), timeFrom,timeTo);
            while(iterator.hasNext()) {
                KeyValue<Long,AdditiveStatistics> windowKv = iterator.next();
                AdditiveStatistics windowValue = windowKv.value;

                Transactions10KEnhanced enhancedTrans = new Transactions10KEnhanced();
                enhancedTrans.setAccountNumber(event.getAccountNumber());
                enhancedTrans.setAccountTypeDesc(event.getAccountTypeDesc());
                enhancedTrans.setPartyNumber(event.getPartyNumber());
                enhancedTrans.setPrimaryMediumDesc(event.getPrimaryMediumDesc());
                enhancedTrans.setSecondaryMediumDesc(event.getSecondaryMediumDesc());
                enhancedTrans.setTransactionKey(event.getTransactionKey());
                enhancedTrans.setCurrencyAmount(event.getCurrencyAmount());
                enhancedTrans.setTransDate(event.getTransDate());
                enhancedTrans.setSum2d(windowKv.value.getSum());

                context.forward(event.getPartyNumber(),enhancedTrans);
            }
            iterator.close();
        }

        @Override
        public void punctuate(long timestamp) {
        }

        @Override
        public void close() {

        }

    }
}
