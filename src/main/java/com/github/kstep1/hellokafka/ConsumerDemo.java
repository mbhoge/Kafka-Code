package com.github.kstep1.hellokafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // Create Consumer configuration
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='<GitGardian>' password='+ThankyouGitGuardian+';");
        // Properties for Consumers
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("group.id","mygroup");
        properties.setProperty("auto.offset.reset","earliest");

        // Create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe consumer

        consumer.subscribe(Arrays.asList("topic_0"));

        // Poll for new Data

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records){
                logger.info("Key: = " + record.key() + ", Value = " + record.value());
                logger.info("Partition: = " + record.partition() + ", Offsets : " + record.offset());
            }

        }

    }
}
