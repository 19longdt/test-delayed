package local.demo.thread_delay;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DelayedSymbolCacheAdapter {

    private final DelayWorkerManager mainWorker;
    private final DelayWorkerManager quoteWorker;

    private final Map<String, String> historicalMap = new ConcurrentHashMap<>();
//    private final Map<String, Deque<String>> historicalMapAll = new ConcurrentHashMap<>();
    private final Map<String, ChunkQueue> historicalMapAll = new ConcurrentHashMap<>();

    private final Map<String, String> quoteMap = new ConcurrentHashMap<>();
//    private final Map<String, Deque<String>> quoteMapAll = new ConcurrentHashMap<>();
    private final Map<String, ChunkQueue> quoteMapAll = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    private final MemoryGuardian guardian;

    public DelayedSymbolCacheAdapter(MemoryGuardian guardian) {
        this.guardian = guardian;
        this.mainWorker = new DelayWorkerManager("MainWorker", 5000, this::processMain);
        this.quoteWorker = new DelayWorkerManager("QuoteWorker", 5000, this::processQuote);
    }

    @PostConstruct
    public void register() {
        guardian.registerManagers(List.of(mainWorker, quoteWorker));
    }

    public void processMain(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            ChunkQueue queue = historicalMapAll.get(entry.getKey());
            if (queue != null) {
                Msg msg = queue.poll();
                if (msg != null) {
                    String value = new String(msg.value(), StandardCharsets.UTF_8);
                    historicalMap.put(entry.getKey(), value);
                }
                if (queue.isEmpty()) {
                    historicalMapAll.remove(entry.getKey());
                    log.warn("[ZMQ] Empty historical map for key: {}", entry.getKey());
                }
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    public void processQuote(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            ChunkQueue queue = quoteMapAll.get(entry.getKey());
            if (queue != null) {
                Msg msg = queue.poll();
                if (msg != null) {
                    String value = new String(msg.value(), StandardCharsets.UTF_8);
                    quoteMap.put(entry.getKey(), value);
                }
                if (queue.isEmpty()) {
                    quoteMapAll.remove(entry.getKey());
                    log.warn("[ZMQ] Empty quote map for key: {}", entry.getKey());
                }
            }
        } finally {
            entry = null;
            recordProcessingTime(start);
        }
    }

    public void pushHistory(String key, String value, long delayMs) {
        totalReceived.incrementAndGet();
//        historicalMapAll.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>()).addLast(value);
//        historicalMap.put(key, value);


        ChunkQueue queue = historicalMapAll.computeIfAbsent(key, k -> new ChunkQueue(8192));

        queue.add(value.getBytes(StandardCharsets.UTF_8));

        mainWorker.submit(key, delayMs);
    }

    public void pushQuote(String key, String value, long delayMs) {
        totalReceived.incrementAndGet();
//        quoteMapAll.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>()).addLast(value);
//        quoteWorker.submit(key, delayMs);
        ChunkQueue queue = quoteMapAll.computeIfAbsent(key, k -> new ChunkQueue(8192));

        queue.add(value.getBytes(StandardCharsets.UTF_8));

        quoteWorker.submit(key, delayMs);
    }

    private void recordProcessingTime(long startNano) {
        long durationMs = (System.nanoTime() - startNano) / 1_000_000;
        totalProcessed.incrementAndGet();
        totalProcessingTimeMs.addAndGet(durationMs);
    }

    @Scheduled(fixedDelay = 10000)
    public void logStats() {
        int received = totalReceived.get();
        int processed = totalProcessed.get();
        long totalTime = totalProcessingTimeMs.get();
        double avgTime = processed > 0 ? (double) totalTime / processed : 0.0;

        log.info("[MapSize] historicalMap={}, historicalMapAll={}, quoteMap={}, quoteMapAll={}",
                historicalMap.size(), historicalMapAll.size(),
                quoteMap.size(), quoteMapAll.size());

        log.info("📊 Stats: received={}, processed={}, totalTime={} ms, avgTime={} ms/msg",
                received, processed, totalTime, String.format("%.2f", avgTime));

        if (historicalMapAll.isEmpty() && quoteMapAll.isEmpty()) {
            quoteMap.clear();
            historicalMap.clear();
            historicalMapAll.clear();
            quoteMapAll.clear();
        }
    }
}
