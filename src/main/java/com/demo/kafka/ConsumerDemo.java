package com.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServer= "127.0.0.1:9092";
        String groupID="my-fourth-application";
        String topic= "first_topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //subscribe consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord <String, String> record: records){
                logger.info(" Key: " +record.key() + " Value: " + record.value() +
                        " Partition :" +record.partition() + " Offset : " +record.offset());
            }
        }


    }


}
