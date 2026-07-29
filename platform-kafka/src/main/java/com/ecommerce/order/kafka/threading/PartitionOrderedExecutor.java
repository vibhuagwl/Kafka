package com.ecommerce.order.kafka.threading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Multi-threading model for Kafka consumers (interview staple).
 *
 * <pre>
 * Listener poll thread (1 per concurrency) ──► per-partition queue ──► worker (virtual thread)
 * </pre>
 *
 * <p>Preserves <b>per-partition ordering</b> while allowing parallel partitions.
 * Back-pressure via bounded queue + semaphore (pause container when saturated — see admin).
 */
public class PartitionOrderedExecutor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionOrderedExecutor.class);

    private final ConcurrentHashMap<Integer, LinkedBlockingQueue<Runnable>> partitionQueues =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Thread> partitionWorkers = new ConcurrentHashMap<>();
    private final ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore inFlight = new Semaphore(256);
    private volatile boolean running = true;

    public void submit(int partition, Runnable task) {
        if (!running) {
            throw new RejectedExecutionException("PartitionOrderedExecutor stopped");
        }
        try {
            if (!inFlight.tryAcquire(5, TimeUnit.SECONDS)) {
                throw new RejectedExecutionException("Back-pressure: in-flight limit reached");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("Interrupted waiting for permit", e);
        }

        LinkedBlockingQueue<Runnable> queue = partitionQueues.computeIfAbsent(partition, p -> {
            LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<>(1000);
            Thread worker = Thread.ofVirtual().name("kafka-part-" + p).start(() -> drain(p, q));
            partitionWorkers.put(p, worker);
            return q;
        });

        Runnable wrapped = () -> {
            try {
                task.run();
            } finally {
                inFlight.release();
            }
        };

        if (!queue.offer(wrapped)) {
            inFlight.release();
            throw new RejectedExecutionException("Back-pressure: partition queue full p=" + partition);
        }
    }

    public <T> CompletableFuture<Void> submitAsync(int partition, T payload, Consumer<T> consumer) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        submit(partition, () -> {
            try {
                consumer.accept(payload);
                future.complete(null);
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    private void drain(int partition, LinkedBlockingQueue<Runnable> queue) {
        while (running || !queue.isEmpty()) {
            try {
                Runnable task = queue.poll(500, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("partition worker interrupted p={}", partition);
                return;
            } catch (Exception ex) {
                log.error("partition worker error p={}", partition, ex);
            }
        }
    }

    public int availablePermits() {
        return inFlight.availablePermits();
    }

    @Override
    public void close() {
        running = false;
        virtualExecutor.shutdownNow();
        partitionWorkers.values().forEach(Thread::interrupt);
    }
}
