package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Create properties object for Producer
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 20; i < 30; i++){
            // Create the ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("sample-topic", "key_"+i, "value_"+i);

            // Send Data - Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        logger.info("\nReceived record metadata. \n" +
                                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error Occurred", e);
                    }
                }
            });
        }


        // flush and close producer
        producer.flush();
        producer.close();
    }
}
