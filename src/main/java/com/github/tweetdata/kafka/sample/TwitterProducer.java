package com.github.tweetdata.kafka.sample;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "We4tXJJJsfDErY6N4XZ8dDBWf";
    String consumerSecret = "vQ5DWM0mikHgjdAiJXI05Myn3IWl67S654b1g4nLV9v93XoDFh";
    String token = "97631035-QZ9zFTaJd20xfAkmraxThfZesF137n6Jq6iBl3nb1";
    String secret = "04BOXIhghCS8tejdogSlqC3pggEwL02WhbxOqG0Ms3kJF";
    ArrayList<String> terms = Lists.newArrayList("Covid19");

    public TwitterProducer(){}
    public static void main(String[] args) {
        System.out.println("Test Message");
        new TwitterProducer().run();
    }
    public void run(){
        logger.info("Set up");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();
        // Kafka Producer
        KafkaProducer<String,String> producer = createKafkaProducer();
        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           logger.info("Stopping Application");
           logger.info("Shutdown client from twitter");
           client.stop();
           logger.info("Closing producer");
           producer.close();
           logger.info("Done !!");
        }));

        // loop to choose to send the tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!= null){
                            logger.info("Something bad happen", e);
                        }
                    }
                });
            }
        }
        logger.info("End Of Application");
    }

    public Client createTwitterClient(BlockingQueue msgQueue){


/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        //ArrayList<Long> followings = Lists.newArrayList(1234L, 566788L);
        //ArrayList<String> terms = Lists.newArrayList("twitter", "api");

        // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer(){
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
        return producer;
    }

}
