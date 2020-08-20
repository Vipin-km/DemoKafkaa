package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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

    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
            Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
            String bootstrapServer= "127.0.0.1:9092";
            String groupID="my-fourth-application";
            String topic= "first_topic";
            //latch for dealing multiple threead
            CountDownLatch latch = new CountDownLatch(1);


            //create the consumer runnable
            logger.info("creating consumer thread");
            Runnable myConsumerThread= new ConsumerThread(
                    latch,
                    topic,
                    bootstrapServer,
                    groupID
            );
            //start the thread
            Thread myThread= new Thread(myConsumerThread);
            myThread.start();

            //add Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }

        ));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got intrrupted",e);
        }finally {
            logger.info("Application is closing");
        }
    }


    public class ConsumerThread implements Runnable{

        private final CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch latch, String topic,
                               String bootstrapServer,String groupID){
            this.latch = latch;

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            //create consumer
             consumer = new KafkaConsumer<String, String>(props);
             //subscribe consumer to our topic
            consumer.subscribe(Arrays.asList(topic));

        }
        @Override
        public void run(){
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(" Key: " + record.key() + " Value: " + record.value() +
                                " Partition :" + record.partition() + " Offset : " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Recieved Shutdown signal!");
            }finally {
                consumer.close();
                //tell our main ocde we are done with consumer
                latch.countDown();
            }

        }
        public void shutdown(){
            //special method to interrupt consumer.poll()
            //it'll throw the exception WakeUpException
            consumer.wakeup();

        }
    }
}
