package com.niyanchun.consumer.multithread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author NiYanchun
 **/
public class Main {
  public static void main(String[] args) throws InterruptedException {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final String topic = "test";
    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    singleThread.submit(new PollThread(topic, config, Executors.newFixedThreadPool(2)));
    singleThread.shutdown();
    singleThread.awaitTermination(1024, TimeUnit.DAYS);
  }
}
