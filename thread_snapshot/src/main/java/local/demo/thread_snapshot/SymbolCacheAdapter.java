package local.demo.thread_snapshot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class SymbolCacheAdapter {
    private final Map<String, Deque<String>> historicalMap = new ConcurrentHashMap<>();

    private final Map<String, String> quoteMap = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);


    public void pushHistory(String key, String value) {
        totalReceived.incrementAndGet();
        historicalMap
                .computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>())
                .addLast(value);
    }

    public void pushQuote(String key, String value) {
        totalReceived.incrementAndGet();
        quoteMap.put(key, value);
    }

    @Scheduled(fixedDelay = 60000)
    public void logStats() {
        int received = totalReceived.get();
        int processed = totalProcessed.get();
        long totalTime = totalProcessingTimeMs.get();
        double avgTime = processed > 0 ? (double) totalTime / processed : 0.0;

        log.info("[MapSize] historicalMap={}, quoteMap={}", historicalMap.size(), quoteMap.size());

        log.info("📊 Stats: received={}, processed={}, totalTime={} ms, avgTime={} ms/msg",
                received, processed, totalTime, String.format("%.2f", avgTime));
    }
}
