package com.sas.kafka.aggrs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.sas.kafka.aggrs.Constants
import com.sas.kafka.aggrs.engine.EventTimeStampExtractor
import groovy.sql.Sql
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.StreamsConfig

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

class LoadTransactions10kToKafkaJson {

    static void main(String[] args) {
        final long FOUR_HOURS_MILLI = 4 * 60*60;
        String topic = "transactions10kJson"
        Properties properties = new Properties();
        properties.put("bootstrap.servers", Constants.KAFKA_BROKER);
        properties.put("acks", "1");
        properties.put("schema.registry.url", Constants.SCHEMA_REGISTRY_URL);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", JsonSerializer.class.getName());
        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeStampExtractor.class);

        Producer producer = new KafkaProducer<String, JsonNode>(properties);
        ObjectMapper mapper = new ObjectMapper();

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
                Map<String,Object> rowMap = rowToMap(it)
                JsonNode rowJson = mapper.valueToTree(rowMap);
                ProducerRecord record = new ProducerRecord(topic,rowMap.partyNumber,rowJson)
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

    static Map<String,Object> rowToMap(row) {
        Map<String,Object> outMap = [:]
        outMap.__eventDate =  row.date_key.getTime() - TimeUnit.HOURS.toMillis(4)
        outMap.accountNumber = row.account_number
        outMap.accountTypeDesc = row.account_type_desc
        outMap.partyNumber = row.party_number
        outMap.primaryMediumDesc = row.primary_medium_desc
        outMap.secondaryMediumDesc = row.secondary_medium_desc
        outMap.currencyAmount = row.currency_amount
        outMap.transactionKey = row.transaction_key

        return outMap
    }
}
