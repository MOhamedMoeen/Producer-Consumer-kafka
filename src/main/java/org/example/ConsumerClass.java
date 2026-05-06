package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerClass {
    public static void main() {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","group1");
        props.put("auto.offset.reset", "earliest");
        Consumer<String,String> consumer = new KafkaConsumer<>(props);
        List<String> list = new ArrayList<>();
        list.add("barcelona");
        consumer.subscribe(list);
        System.out.println("Consumer subuscribedddd");

        int rec = 0;
        List<Long> responses = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();
        while(rec<10000){
            long start = System.currentTimeMillis();

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(10));
            if(!records.isEmpty()){
                long responseTimeInMillis = (System.currentTimeMillis() - start)/records.count();
                for(int j = 0;j<records.count();j++)
                    responses.add(responseTimeInMillis);
            }
            for(ConsumerRecord<String,String> record: records){
                rec++;
                System.out.println(
                        "Topic: " + record.topic() +
                                " | Partition: " + record.partition() +
                                " | Offset: " + record.offset()
                );

                System.out.println("Message:\n" + record.value());
                String[] parts = record.value().split("\\|", 2);
                latencies.add(System.currentTimeMillis()-Long.parseLong(parts[0]));
            }
        }
//        for(long response:responses) {
//            System.out.println(response+" ");
//        }
//        System.out.println("\n");
        Collections.sort(responses);
        Collections.sort(latencies);
        System.out.println("Median is "+responses.get(responses.size()/2));
        System.out.println("Median Latency is "+latencies.get(latencies.size()/2));
//        for(long latency : latencies){
//            System.out.println(latency+" ");
//        }
//        System.out.println("\n");


    }
}
