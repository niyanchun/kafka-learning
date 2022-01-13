package com.niyanchun.consumer.multithread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author NiYanchun
 **/
@Slf4j
public class ProcessThread implements Runnable {
  private final List<ConsumerRecord<String, String>> records;
  private final AtomicLong currentOffset = new AtomicLong();
  private volatile boolean stopped = false;
  private volatile boolean started = false;
  private volatile boolean finished = false;
  private final CompletableFuture<Long> completion = new CompletableFuture<>();
  private final ReentrantLock startStopLock = new ReentrantLock();

  public ProcessThread(List<ConsumerRecord<String, String>> records) {
    this.records = records;
  }

  @Override
  public void run() {
    startStopLock.lock();
    try {
      if (stopped) {
        return;
      }
      started = true;
    } finally {
      startStopLock.unlock();
    }

    for (ConsumerRecord<String, String> record : records) {
      if (stopped) {
        break;
      }
      // process record here and make sure you catch all exceptions;
      System.out.println(Thread.currentThread().getName() + ":" + record);
      currentOffset.set(record.offset() + 1);
    }
    finished = true;
    completion.complete(currentOffset.get());
  }

  public long getCurrentOffset() {
    return currentOffset.get();
  }

  public void stop() {
    startStopLock.lock();
    try {
      this.stopped = true;
      if (!started) {
        finished = true;
        completion.complete(currentOffset.get());
      }
    } finally {
      startStopLock.unlock();
    }
  }

  public long waitForCompletion() {
    try {
      return completion.get();
    } catch (InterruptedException | ExecutionException e) {
      return -1;
    }
  }

  public boolean isFinished() {
    return finished;
  }
}
