package com.github.ytian.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String value = "hello world" + i;
            String key = "id_" + i;
            try {
                kafkaProducer(producer, topic, key, value);
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // flush data
        producer.flush();
        // flush and close
        producer.close();
    }

    public static void kafkaProducer(KafkaProducer<String, String> producer, String topic, String key, String value) throws ExecutionException, InterruptedException {
        // create producer record
        ProducerRecord<String, String> record
                = new ProducerRecord<String, String>(topic, key, value);

        logger.info("Key: " + key); // log the key
        // same key always goes to the same partition

        // send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // execute every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // record was succesffuly sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        }).get(); // block the .send() to make it synchronous - don't do this in production
    }
}
