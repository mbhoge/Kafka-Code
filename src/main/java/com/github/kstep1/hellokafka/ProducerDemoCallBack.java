package com.github.kstep1.hellokafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        System.out.println("Hello World");
        Properties properties = new Properties();
        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);

        properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='<GitGardian>' password='+ThankyouGitGuardian+';");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("sasl.mechanism","PLAIN");
        /*
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

         */

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i<10; i++) {
            // Create Producer Records
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic_0", "Hello World" + Integer.toString(i));
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received New Matadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition " + recordMetadata.partition() + "\n" +
                                "Offset " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error: ", e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
