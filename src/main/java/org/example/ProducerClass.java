package org.example;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProducerClass {
    public static void main() throws IOException, ExecutionException, InterruptedException {
        String content = Files.readString(Paths.get("src/main/resources/message.txt"));
//        System.out.println(content);
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        List<Long> responses = new ArrayList<>();
        for(int i = 0;i<10000;i++){
            long start = System.currentTimeMillis();
            ProducerRecord<String,String>record = new ProducerRecord<>("barcelona",start+"|"+content);
            producer.send(record,((metadata, exception) ->{
                if(exception == null){
                    System.out.println("producer sent to topic "+metadata.topic() + "partition "+metadata.partition());
                }
                else {
                    exception.printStackTrace();
                }
            } )).get();
            long responseTimeInMillis = System.currentTimeMillis() - start;
            responses.add(responseTimeInMillis);
        }
        Collections.sort(responses);

        System.out.println("Median is "+responses.get(499));



        producer.close();
    }
}
