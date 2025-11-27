package local.demo.thread_delay;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Log4j2
public class DelayWorkerManager {
    private final String name;
    private final DelayQueue<DelayedEntry> delayQueue = new DelayQueue<>();
    private final AtomicReference<Thread> worker = new AtomicReference<>();
    private final long idleTimeoutMs;
    private final Consumer<DelayedEntry> handler;

    public DelayWorkerManager(String name, long idleTimeoutMs, Consumer<DelayedEntry> handler) {
        this.name = name;
        this.idleTimeoutMs = idleTimeoutMs;
        this.handler = handler;
    }

    public void submit(String key, long delayMs) {
        delayQueue.offer(new DelayedEntry(key, delayMs));
        startWorkerIfNeeded();
    }

    private void startWorkerIfNeeded() {
        Thread existing = worker.get();
        if (existing != null && existing.isAlive()) return;

        Thread newWorker = new Thread(this::processLoop, name);
        newWorker.setDaemon(true);
        if (worker.compareAndSet(existing, newWorker)) {
            newWorker.start();
        }
    }

    private void processLoop() {
        try {
            long lastActive = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                DelayedEntry entry = delayQueue.poll(1, TimeUnit.SECONDS);

                if (entry != null) {
                    lastActive = System.currentTimeMillis();
                    handler.accept(entry); // callback xử lý logic riêng
                } else if (delayQueue.isEmpty() &&
                        (System.currentTimeMillis() - lastActive > idleTimeoutMs)) {
                    log.info("[DelayQueue] {} idle, close", name);
//                    System.gc();
                    break;
                }
                entry = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            worker.set(null);
            System.out.printf("[%s] worker stopped%n", name);
        }
    }

    public boolean isRunning() {
        Thread t = worker.get();
        return t != null && t.isAlive();
    }
}