package com.niyanchun.consumer.multithread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author NiYanchun
 **/
@Slf4j
public class PollThread implements Runnable, ConsumerRebalanceListener {
  private final KafkaConsumer<String, String> consumer;
  private final ExecutorService processThreadPool;
  private final Map<TopicPartition, ProcessThread> activeTasks = new HashMap<>();
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private long lastCommitTime = System.currentTimeMillis();

  public PollThread(String topic, Properties config, ExecutorService processThreadPool) {
    this.processThreadPool = processThreadPool;
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Collections.singleton(topic), this);
  }

  @Override
  public void run() {
    try {
      // 主流程
      while (!stopped.get()) {
        // 消费数据
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10_000));
        // 按partition分组，分发数据给处理线程池
        handleFetchedRecords(records);
        // 检查正在处理的线程
        checkActiveTasks();
        // 提交offset
        commitOffsets();
      }

    } catch (WakeupException we) {
      if (!stopped.get()) {
        throw we;
      }
    } finally {
      consumer.close();
    }
  }

  private void handleFetchedRecords(ConsumerRecords<String, String> records) {
    if (records.count() > 0) {
      List<TopicPartition> partitionsToPause = new ArrayList<>();
      // 按partition分组
      records.partitions().forEach(partition -> {
        List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);
        // 提交一个分区的数据给处理线程池
        ProcessThread processThread = new ProcessThread(consumerRecords);
        processThreadPool.submit(processThread);
        // 记录分区与处理线程的关系，方便后面查询处理状态
        activeTasks.put(partition, processThread);
      });
      // pause已经在处理的分区，避免同个分区的数据被多个线程同时消费，从而保证分区内数据有序处理
      consumer.pause(partitionsToPause);
    }
  }

  /**
   * 该方法主要是找到已经处理完成的partition，获取offset放到待提交队列里面
   */
  private void checkActiveTasks() {
    List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
    activeTasks.forEach((partition, processThread) -> {
      if (processThread.isFinished()) {
        finishedTasksPartitions.add(partition);
      }
      long offset = processThread.getCurrentOffset();
      if (offset > 0) {
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
      }
    });

    finishedTasksPartitions.forEach(activeTasks::remove);
    consumer.resume(finishedTasksPartitions);
  }

  private void commitOffsets() {
    try {
      long currentMillis = System.currentTimeMillis();
      if (currentMillis - lastCommitTime > 5000) {
        if (!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
        }
        lastCommitTime = currentMillis;
      }
    } catch (Exception e) {
      log.error("Failed to commit offsets!", e);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // 1. stop all tasks handling records from revoked partitions
    Map<TopicPartition, ProcessThread> stoppedTask = new HashMap<>();
    for (TopicPartition partition : partitions) {
      ProcessThread processThread = activeTasks.remove(partition);
      if (processThread != null) {
        processThread.stop();
        stoppedTask.put(partition, processThread);
      }
    }

    // 2. wait for stopped task to complete processing of current record
    stoppedTask.forEach((partition, processThread) -> {
      long offset = processThread.waitForCompletion();
      if (offset > 0) {
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
      }
    });

    // 3. collect offsets for revoked partitions
    Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
    for (TopicPartition partition : partitions) {
      OffsetAndMetadata offsetAndMetadata = offsetsToCommit.remove(partition);
      if (offsetAndMetadata != null) {
        revokedPartitionOffsets.put(partition, offsetAndMetadata);
      }
    }

    // 4. commit offsets for revoked partitions
    try {
      consumer.commitSync(revokedPartitionOffsets);
    } catch (Exception e) {
      log.warn("Failed to commit offsets for revoked partitions!", e);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    // 如果分区之前没有pause过，那执行resume就不会有什么效果
    consumer.resume(partitions);
  }

  public void stopConsuming() {
    stopped.set(true);
    consumer.wakeup();
  }
}
