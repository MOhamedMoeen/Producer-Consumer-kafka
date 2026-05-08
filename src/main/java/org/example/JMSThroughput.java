package org.example;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class JMSThroughput {

    public static void main() throws IOException, InterruptedException {
        String content = Files.readString(Paths.get("src/main/resources/message.txt"));
        int throughput = 1000;
        while (true) {
            System.out.println("\nTesting throughput: " + throughput + " msg/sec");
            boolean success = throughputTest(content, throughput, throughput*10);

            if (!success) {
                System.out.println("\nMax throughput reached: " + (throughput / 2) + " msg/sec");
                break;
            }
            System.out.println("Throughput " + throughput + " msg/sec: OK");
            throughput *= 2;
        }
    }

    public static boolean throughputTest(String content, int throughput, int messages) throws InterruptedException {
        int threads = Runtime.getRuntime().availableProcessors()*4;
        int messagesPerThread = messages / threads;
        CyclicBarrier startBarrier = new CyclicBarrier(threads);
        AtomicInteger failed_tot = new AtomicInteger(0);
        AtomicInteger sent = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CountDownLatch downLatch = new CountDownLatch(threads);

        long totalWindowMs = (long) ((messages / (double) throughput) * 1000);

        for (int t = 0; t < threads; t++) {
            int msgCount;
            if (t + 1 == threads) {
                msgCount = messages - messagesPerThread * t;
            } else {
                msgCount = messagesPerThread;
            }

            executorService.submit(() -> {
                Connection connection = null;
                try {
                    ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
                    connection = factory.createConnection();
                    connection.start();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue queue = session.createQueue("barcelona");
                    MessageProducer producer = session.createProducer(queue);

                    double T = 1000.0 / (throughput / (double) threads);
                    long sleep = (long) (T - 0.2 * T);

                    startBarrier.await();

                    long threadStart = System.currentTimeMillis();

                    int failed = 0;
                    for (int i = 0; i < msgCount; i++) {
                        // Deadline for this message: threadStart + i * T
                        long deadline = threadStart + (long)(i * T);
                        long now = System.currentTimeMillis();

                        // Already past deadline before even sending = failure
                        if (now > deadline + (long)T) {
                            failed++;
                            continue;
                        }

                        try {
                            TextMessage message = session.createTextMessage(content);
                            producer.send(message);
                            sent.incrementAndGet();
                        } catch (Exception e) {
                            failed++;
                        }

                        long elapsed = System.currentTimeMillis() - threadStart - (long)(i * T);
                        long remaining = sleep - elapsed;
                        if (remaining > 0) {
                            Thread.sleep(remaining);
                        }
                    }

                    failed_tot.addAndGet(failed);
                    producer.close();
                    session.close();

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (connection != null) {
                        try { connection.close(); } catch (JMSException e) {}
                    }
                    downLatch.countDown();
                }
            });
        }

        downLatch.await();
        executorService.shutdown();
        System.out.println("Threads used:     " + threads);
        System.out.println("Total sent:       " + sent.get());
        System.out.println("Failed requests:  " + failed_tot.get());
        double failureRate = (double) failed_tot.get() / messages;
        return failureRate <= 0.1;
    }


}
