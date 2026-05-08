package org.example;

import jakarta.jms.Connection;
import jakarta.jms.Queue;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JMSProducer {
    public static void main() throws JMSException, IOException {
        ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("barcelona");
        MessageProducer producer = session.createProducer(queue);
        List<Long> responses = new ArrayList<>();
        String content = Files.readString(Paths.get("src/main/resources/message.txt"));
        for(int i = 0;i<10000;i++){
            long start = System.currentTimeMillis();
            TextMessage message = session.createTextMessage(start+"|"+content);
            producer.send(message);
            long responseTimeInMillis = System.currentTimeMillis() - start;
            responses.add(responseTimeInMillis);
            System.out.println("message sent");

        }

        producer.close();
        session.close();
        connection.close();
        Collections.sort(responses);

        System.out.println("Median is "+responses.get(499));
    }

}
