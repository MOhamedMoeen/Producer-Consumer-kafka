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
        for(int i = 0;i<1000;i++){
            long start = System.currentTimeMillis();
            Message msg = consumer.receive();
            TextMessage textMessage = (TextMessage) msg;
            long responseTimeInMillis = System.currentTimeMillis() - start;
            responses.add(responseTimeInMillis);
            System.out.println("Received "+textMessage);
        }
        consumer.close();
        session.close();
        connection.close();

        Collections.sort(responses);

        System.out.println("Median is "+responses.get(499));
    }
}
