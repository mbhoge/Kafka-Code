package com.github.kstep1.hellokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='3D4YKEZMJSEFMDJ2' password='+GHhCSJ8MrzvRo5dBjeQa42RKZIvO1iyLvsCSNyrZQTnlKFh83r+mnrBlpVNElAo';");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("sasl.mechanism","PLAIN");
        /*
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        */
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Records
        ProducerRecord <String,String> record = new ProducerRecord<String, String>("topic_0","Hello World");
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
