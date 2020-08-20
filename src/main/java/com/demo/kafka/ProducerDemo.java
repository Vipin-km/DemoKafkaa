package com.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServer= "127.0.0.1:9092";

        //setting producers property
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //creating producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


            //cerate producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello_world");

            //send the data --asynchronous
            producer.send(record);
            //flush and close the producer
            producer.flush();
            producer.close();




    }
}
