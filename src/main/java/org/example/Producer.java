package org.example;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Producer {
    public static void main() throws IOException {
        String content = Files.readString(Paths.get("src/main/resources/message.txt"));
//        System.out.println(content);
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String,String>record = new ProducerRecord<>("barcelona",content);
        producer.send(record,((metadata, exception) ->{
            if(exception == null){
                System.out.println("producer sent to topic "+metadata.topic() + "partition "+metadata.partition());
            }
            else {
                exception.printStackTrace();
            }
        } ));
        producer.close();
    }
}
