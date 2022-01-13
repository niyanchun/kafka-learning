package com.niyanchun.consumer.perthread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author NiYanchun
 **/
public class ThreadPerConsumerModel {
  public static void main(String[] args) throws InterruptedException {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final String topic = "test";
    int partitionNum = getPartitionNum(topic);
    final int threadNum = 4;
    int partitionNumPerThread = (partitionNum <= threadNum) ? 1 : partitionNum / threadNum + 1;

    ExecutorService threadPool = Executors.newFixedThreadPool(partitionNum);
    List<TopicPartition> topicPartitions = new ArrayList<>(partitionNumPerThread);
    for (int i = 0; i < partitionNum; i++) {
      topicPartitions.add(new TopicPartition(topic, i));
      if ((i + 1) % partitionNumPerThread == 0) {
        threadPool.submit(new Task(new ArrayList<>(topicPartitions), config));
        topicPartitions.clear();
      }
    }
    if (!topicPartitions.isEmpty()) {
      threadPool.submit(new Task(new ArrayList<>(topicPartitions), config));
    }

    threadPool.shutdown();
    threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  static class Task implements Runnable {
    private final List<TopicPartition> topicPartitions;
    private final Properties config;

    public Task(List<TopicPartition> topicPartitions, Properties config) {
      this.topicPartitions = topicPartitions;
      this.config = config;
    }

    @Override
    public void run() {
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
      System.out.println(topicPartitions);
      consumer.assign(topicPartitions);
      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10_000));
          for (ConsumerRecord<String, String> record : records) {
            System.out.println(record);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        consumer.close();
      }
    }
  }

  public static int getPartitionNum(String topic) {
    return 8;
  }
}
