package org.example;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class JMSConsumerThroughput {

    public static void main() throws InterruptedException {
        int throughput = 1000;
        while (true) {
            System.out.println("\nTesting consumer throughput: " + throughput + " msg/sec");
            boolean success = throughputTest(throughput, throughput * 10);

            if (!success) {
                System.out.println("\nMax throughput reached: " + (throughput / 2) + " msg/sec");
                break;
            }
            System.out.println("Throughput " + throughput + " msg/sec: OK");
            throughput *= 2;
        }
    }

    public static boolean throughputTest(int throughput, int messages) throws InterruptedException {
        int threads = Runtime.getRuntime().availableProcessors()*2;
        int messagesPerThread = messages / threads;
        CyclicBarrier startBarrier = new CyclicBarrier(threads);
        AtomicInteger failed_tot = new AtomicInteger(0);
        AtomicInteger received = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CountDownLatch downLatch = new CountDownLatch(threads);

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
                    MessageConsumer consumer = session.createConsumer(queue);

                    double T = 1000.0 / (throughput / (double) threads);
                    long sleep = (long) (T - 0.2 * T);

                    startBarrier.await();

                    long threadStart = System.currentTimeMillis();

                    int failed = 0;
                    for (int i = 0; i < msgCount; i++) {
                        long deadline = threadStart + (long)(i * T);
                        long now = System.currentTimeMillis();

                        if (now > deadline + (long)T) {
                            failed++;
                            continue;
                        }

                        long receiveTimeout = Math.max(1, (deadline + (long)T) - System.currentTimeMillis());
                        try {
                            Message message = consumer.receive(receiveTimeout);
                            if (message == null) {
                                failed++;
                            } else {
                                received.incrementAndGet();
                            }
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
                    consumer.close();
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
        System.out.println("Threads used:      " + threads);
        System.out.println("Total received:    " + received.get());
        System.out.println("Failed requests:   " + failed_tot.get());

        double failureRate = (double) failed_tot.get() / messages;
        return failureRate <= 0.1;
    }
}