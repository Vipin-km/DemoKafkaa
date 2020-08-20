package com.demo.kafka;

import jdk.nashorn.internal.runtime.arrays.ArrayData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServer= "127.0.0.1:9092";
        String topic= "first_topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //assign and seek are mostly used to replay data or fetch a specific message

        //assing
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMesssageTOread= 5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSoFar=0;

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord <String, String> record: records){
                numberOfMessagesReadSoFar +=1;
                logger.info(" Key: " +record.key() + " Value: " + record.value() +
                        " Partition :" +record.partition() + " Offset : " +record.offset());
                if (numberOfMessagesReadSoFar>=numberOfMesssageTOread){
                    keepOnReading=false; //to exit the while loop
                    break;
                }
            }
            logger.info("Exiting the application");

        }


    }




}
