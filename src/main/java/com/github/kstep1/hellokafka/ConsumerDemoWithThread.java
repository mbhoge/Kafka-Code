package com.github.kstep1.hellokafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

       // Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

       /* // Create Consumer configuration
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='<GitGardian>' password='+ThankyouGitGuardian+';");
        // Properties for Consumers
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("group.id","mygroup");
        properties.setProperty("auto.offset.reset","earliest");*/

        // Create consumer

       // KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe consumer

        // consumer.subscribe(Arrays.asList("topic_0"));

        // Poll for new Data

        /*while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records){
                logger.info("Key: = " + record.key() + ", Value = " + record.value());
                logger.info("Partition: = " + record.partition() + ", Offsets : " + record.offset());
            }

        }*/

    }

    private ConsumerDemoWithThread(){

    }

    public void run(){
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThread(latch);
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        // Create Consumer configuration

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            consumer = new KafkaConsumer<String, String>(properties);
            properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
            properties.setProperty("security.protocol","SASL_SSL");
            properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='3D4YKEZMJSEFMDJ2' password='+GHhCSJ8MrzvRo5dBjeQa42RKZIvO1iyLvsCSNyrZQTnlKFh83r+mnrBlpVNElAo';");
            // Properties for Consumers
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer",StringDeserializer.class.getName());
            properties.setProperty("sasl.mechanism","PLAIN");
            properties.setProperty("group.id","mygroup-thread-application");
            properties.setProperty("auto.offset.reset","earliest");
            consumer.subscribe(Arrays.asList("topic_0"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: = " + record.key() + ", Value = " + record.value());
                        logger.info("Partition: = " + record.partition() + ", Offsets : " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received Shutdown Signals");
            }finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown(){
            // the wakeup() method is a spacial method to interrupt consumer.poll()
            // it will throw the WakeUpException
            consumer.wakeup();

        }
    }
}
