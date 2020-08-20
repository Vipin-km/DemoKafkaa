package com.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithCallback {
    public static void main(String[] args) {
        String bootstrapServer= "127.0.0.1:9092";

        final Logger logger=LoggerFactory.getLogger(ProducerDemowithCallback.class);

        //setting producers property
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //creating producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


            //cerate producer record
        for (int i=0;i<=10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello_world" + Integer.toString(i));

            //send the data --asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        //record was sent
                        logger.info("Recived new matadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "TimeStamp:" + recordMetadata.timestamp());

                    } else {

                        logger.error("Error while producing" + e);
                    }
                }
            });
        }
            //flush and close the producer
            producer.flush();
            producer.close();




    }
}
