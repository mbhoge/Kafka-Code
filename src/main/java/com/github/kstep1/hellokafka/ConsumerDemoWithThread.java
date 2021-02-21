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
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Caught Shutdown Hook");
                    ((ConsumerRunnable) myConsumerRunnable).shutdown();
                    try {
                        latch.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application has exited");
                }
        ));

        try {
            latch.wait();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
            e.printStackTrace();
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        // Create Consumer configuration

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch){
            this.latch = latch;
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

            consumer = new KafkaConsumer<String, String>(properties);
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
