package com.github.kstep1.hellokafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentType.JSON;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient(){

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        String hostname = "trade-data-test-6730815939.us-east-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "57ow4y1jgb"; // needed only for bonsai
        String password = "aynnmiq5j"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson){
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
    public static KafkaConsumer<String, String> createConsumer(String topic){
        // create consumer configs

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","pkc-l9wvm.ap-southeast-1.aws.confluent.cloud:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='<GitGardian>' password='+ThankyouGitGuardian+';");
        // Properties for Consumers
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("group.id","kafka-demo-elasticsearch");
        properties.setProperty("auto.offset.reset","earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        RestHighLevelClient client = createClient();
        /*String jsonString = "{\"foo\": \"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter","tweets").source(jsonString, JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);*/
        //String id = indexResponse.getId();

        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records){

                // 2 strategies
                // kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                // String id = extractIdFromTweet(record.value());
                String jsonString = record.value();

                // where we insert data into ElasticSearch

                /**
                 * Uncomment this code if you are using elastic search version < 7.0
                 IndexRequest indexRequest = new IndexRequest(
                 "twitter",
                 "tweets",
                 id // this is to make our consumer idempotent
                 ).source(record.value(), XContentType.JSON);
                 */

                /** Added for ElasticSearch >= v7.0
                 * The API to add data into ElasticSearch has slightly changes
                 * and therefore we must use this new method.
                 * the idea is still the same
                 * read more here: https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
                 * uncomment the code above if you use ElasticSearch 6.x or less
                 */

                IndexRequest indexRequest = new IndexRequest("twitter","tweets")
                        .source(record.value(), XContentType.JSON); // this is to make our consumer idempotent
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)

            }

           /* if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }*/
        }

        //logger.info(id);

       // client.close();


    }
}
