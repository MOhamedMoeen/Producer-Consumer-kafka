package org.example;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JMSConsumer  {
    public static void main() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("barcelona");
        MessageConsumer consumer = session.createConsumer(queue);
        List<Long> responses = new ArrayList<>();
        List<Long> latencies = new ArrayList<>();
        for(int i = 0;i<10000;i++){
            long start = System.currentTimeMillis();
            Message msg = consumer.receive();
            TextMessage textMessage = (TextMessage) msg;
            long cur = System.currentTimeMillis();
            long responseTimeInMillis = cur - start;

            String[] parts =
                    textMessage.getText().split("\\|", 2);
            long latency =cur-Long.parseLong(parts[0]);
            responses.add(responseTimeInMillis);
            latencies.add(latency);
            System.out.println("Received "+textMessage);
        }
        consumer.close();
        session.close();
        connection.close();

        Collections.sort(responses);
        Collections.sort(latencies);
        System.out.println("Median is "+responses.get(499));
        System.out.println("Median Latency is "+latencies.get(499));
    }
}
