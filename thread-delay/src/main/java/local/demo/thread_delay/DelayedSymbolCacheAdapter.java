package local.demo.thread_delay;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Deque;
import java.util.List;
import java.util.Map;
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
    private final Map<String, Deque<String>> historicalMapAll = new ConcurrentHashMap<>();

    private final Map<String, String> quoteMap = new ConcurrentHashMap<>();
    private final Map<String, Deque<String>> quoteMapAll = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    private final MemoryGuardian guardian;

    public DelayedSymbolCacheAdapter(MemoryGuardian guardian) {
        this.guardian = guardian;
        this.mainWorker = new DelayWorkerManager("MainWorker", 60_000, this::processMain);
        this.quoteWorker = new DelayWorkerManager("QuoteWorker", 60_000, this::processQuote);
    }

    @PostConstruct
    public void register() {
        guardian.registerManagers(List.of(mainWorker, quoteWorker));
    }

    public void processMain(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            Deque<String> values = historicalMapAll.get(entry.getKey());
            if (values != null && !values.isEmpty()) {
                String value = values.pollFirst();
//                historicalMap.computeIfAbsent(entry.getKey(), k -> new ConcurrentLinkedDeque<>()).addFirst(value);
                historicalMap.put(entry.getKey(), value);
            }
            if (values != null && values.isEmpty()) {
                historicalMapAll.remove(entry.getKey());
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    public void processQuote(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            Deque<String> values = quoteMapAll.get(entry.getKey());
            if (values != null && !values.isEmpty()) {
                String value = values.pollFirst();
                quoteMap.put(entry.getKey(), value);
            }
            if (values != null && values.isEmpty()) {
                quoteMapAll.remove(entry.getKey());
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    public void pushHistory(String key, String value, long delayMs) {
        historicalMapAll.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>()).addLast(value);
        totalReceived.incrementAndGet();
        mainWorker.submit(key, delayMs);
    }

    public void pushQuote(String key, String value, long delayMs) {
        quoteMapAll.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>()).addLast(value);
        totalReceived.incrementAndGet();
        quoteWorker.submit(key, delayMs);
    }

    private void recordProcessingTime(long startNano) {
        long durationMs = (System.nanoTime() - startNano) / 1_000_000;
        totalProcessed.incrementAndGet();
        totalProcessingTimeMs.addAndGet(durationMs);
    }

//    @Scheduled(fixedDelay = 60000)
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
            log.info("[Result] historicalMap= {}, quoteMap={}", historicalMap.size(), quoteMap.size());
        }
    }
}
