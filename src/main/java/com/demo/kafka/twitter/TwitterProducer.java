package com.demo.kafka.twitter;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey ="OQRkCFBrwH1vfllG1mZ9kTx2n";
    String  consumerSecret= "znKsoQluOOujask2dj8jRBAHM0BmBsRbo2SvtuH3YSKyYROkzf";
    String token="1654723856-mKsV6j0uRewKRwKEDYr0xQjNsmrLNmryK1dZE7A";
    String secret="4f47UkZ1u9Lalom1ecoqNFU5HVzpGQ1mIOwczg0W01Q1J";

    List<String> terms = Lists.newArrayList("kafka", "java");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    public void run(){
        logger.info("setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        //create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping the application");
            logger.info("Shuting down client fromtwitter..");
            client.stop();
            logger.info("Done!  ");
        }));

        //loop to send the tweet to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.DAYS.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.info("Something bad happened", e);
                        }
                    }
                });
                
            }
        }
        logger.info("End of application");


    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        //** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        // optional: List<Long> followings = Lists.newArrayList(1234L, 566788L);

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        }
    //create kafka producer
    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServer= "127.0.0.1:9092";

        //setting producers property
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //creating producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;

    }


}
