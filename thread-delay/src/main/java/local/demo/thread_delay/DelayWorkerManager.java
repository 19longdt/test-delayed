package local.demo.thread_delay;

import local.demo.thread_delay.monitor.WorkerMonitor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class DelayWorkerManager {

    private final String symbol;
    private final DelayQueue<DelayedEntry> queue = new DelayQueue<>();
    private final AtomicReference<Thread> worker = new AtomicReference<>();
    private final long idleTimeoutMs;
    private final Consumer<DelayedEntry> handler;

    public DelayWorkerManager(String symbol,
                              long idleTimeoutMs,
                              Consumer<DelayedEntry> handler) {
        this.symbol = symbol;
        this.idleTimeoutMs = idleTimeoutMs;
        this.handler = handler;
    }

    public void submit(long delayMs) {
        queue.offer(new DelayedEntry(symbol, delayMs));
        startWorkerIfNeeded();
    }

    private void startWorkerIfNeeded() {
        Thread existing = worker.get();
        if (existing != null && existing.isAlive()) return;

        Thread vt = Thread.ofVirtual().unstarted(this::processLoop);

        if (worker.compareAndSet(existing, vt)) {
            WorkerMonitor.ACTIVE_WORKERS.incrementAndGet();
            vt.start();
            log.info("[{}] Virtual worker started", symbol);
        }
    }

    private void processLoop() {
        try {
            long lastActive = System.currentTimeMillis();

            while (true) {
                DelayedEntry entry = queue.poll(1, TimeUnit.SECONDS);

                if (entry != null) {
                    lastActive = System.currentTimeMillis();
                    handler.accept(entry);
                } else if (queue.isEmpty() &&
                        System.currentTimeMillis() - lastActive > idleTimeoutMs) {
                    WorkerMonitor.ACTIVE_WORKERS.decrementAndGet();
                    log.info("[{}] idle timeout, worker shutting down", symbol);
                    break;
                }
            }
        } catch (InterruptedException ignored) {
        } finally {
            worker.set(null);
            System.out.printf("[%s] worker stopped%n", symbol);
        }
    }

    public boolean isRunning() {
        Thread w = worker.get();
        return w != null && w.isAlive();
    }
}