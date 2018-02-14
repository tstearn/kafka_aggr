import com.sas.kafka.aggrs.Constants
import com.sas.kafka.aggrs.domain.Transactions10TimestampExtractor
import groovy.sql.Sql
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import com.sas.kafkaaggr.domain.Transactions10K
import org.apache.kafka.streams.StreamsConfig

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

class LoadTransactions10kToKafka {

    static void main(String[] args) {
        final long FOUR_HOURS_MILLI = 4 * 60*60;
        String topic = "transactions10k"
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constants.KAFKA_BROKER);
        properties.put("acks", "1");
        properties.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", KafkaAvroSerializer.class.getName());
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, Transactions10TimestampExtractor.class);

        Producer producer = new KafkaProducer<String, Transactions10K>(properties);

        Sql db = Sql.newInstance("jdbc:postgresql://pass1465.na.sas.com:5432/acme",
                                 "dbmsowner",
                                 "Go4thsas",
                                 "org.postgresql.Driver")
        String query = """select date_key, 
                                 account_number, 
                                 account_type_desc, 
                                 party_number,
                                 primary_medium_desc,
                                 secondary_medium_desc,
                                 currency_amount,
                                 transaction_key
                           from vsd_dev_test.transactions10k 
                           where party_number = '10340846'
        """

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        try {
            db.eachRow(query,  {
                //Convert date to UTC time setting ahead 4 hours
                Transactions10K row = new Transactions10K(transDate: it.date_key.getTime() - TimeUnit.HOURS.toMillis(4),
                                                          accountNumber: it.account_number,
                                                          accountTypeDesc: it.account_type_desc,
                                                          partyNumber: it.party_number,
                                                          primaryMediumDesc: it.primary_medium_desc,
                                                          secondaryMediumDesc: it.secondary_medium_desc,
                                                          currencyAmount: it.currency_amount,
                                                          transactionKey: it.transaction_key)
                ProducerRecord record = new ProducerRecord(topic,row.partyNumber,row)
                producer.send(record, { metadata, exception ->
                    if(metadata) {
                        println "partition: ${metadata.partition()}, offset:  ${metadata.offset()}"
                    }
                })
            })
        }
        finally {
            producer.close()
        }
    }
}
