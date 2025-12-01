package local.demo.thread_delay.ringBuffer;

import jakarta.annotation.PostConstruct;
import local.demo.thread_delay.DelayWorkersRegistry;
import local.demo.thread_delay.DelayedEntry;
import local.demo.thread_delay.monitor.MemoryGuardian;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class FixDelayedSymbolCacheAdapter {

    private final DelayWorkersRegistry historyRegistry;
    private final DelayWorkersRegistry quoteRegistry;

    private final Map<String, Deque<String>> historicalMap = new ConcurrentHashMap<>();
    private final Map<String, FixedRingBuffer<byte[]>> historicalRing = new ConcurrentHashMap<>();

    private final Map<String, String> quoteMap = new ConcurrentHashMap<>();
    private final Map<String, FixedRingBuffer<byte[]>> quoteRing = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    private final MemoryGuardian guardian;
    private static final int RING_SIZE = 10_000;

    public FixDelayedSymbolCacheAdapter(MemoryGuardian guardian) {
        this.guardian = guardian;

        // mỗi symbol 1 worker riêng
        this.historyRegistry = new DelayWorkersRegistry(
                60_000,
                this::processMain
        );

        this.quoteRegistry = new DelayWorkersRegistry(
                60_000,
                this::processQuote
        );
    }

    @PostConstruct
    public void register() {
        guardian.registerManagers(List.of(historyRegistry, quoteRegistry));
    }

    // -------------------- PROCESS MAIN ----------------------------

    public void processMain(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            FixedRingBuffer<byte[]> rb = historicalRing.get(entry.getKey());
            if (rb != null) {
                byte[] val = rb.poll();
                if (val != null) {
                    if (entry.getKey().equals("SYM1")) {
                        log.info("process main map " + new String(val));
                    }
                    historicalMap
                            .computeIfAbsent(entry.getKey(), k -> new ConcurrentLinkedDeque<>())
                            .addLast(new String(val));
                } else {
                    log.warn("[ZMQ] value null {}");
                }
                if (rb.isEmpty()) {
                    historicalRing.remove(entry.getKey());
                }
            } else {
                log.warn("[ZMQ] No historical ring found for {}", entry.getKey());
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    // -------------------- PROCESS QUOTE ----------------------------

    public void processQuote(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            FixedRingBuffer<byte[]> rb = quoteRing.get(entry.getKey());
            if (rb != null) {
                byte[] val = rb.poll();
                if (val != null) {
                    quoteMap.put(entry.getKey(), new String(val));
                }
                if (rb.isEmpty()) {
                    quoteRing.remove(entry.getKey());
                }
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    // -------------------- PUSH METHODS ----------------------------

    public void pushHistory(String key, String value, long delayMs) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        historicalRing
                .computeIfAbsent(key, k -> new FixedRingBuffer<>(RING_SIZE))
                .offer(raw);

        totalReceived.incrementAndGet();
        historyRegistry.submit(key, delayMs);  // <-- worker riêng cho từng symbol
//        if (key.equals("SYM1")) {
//            log.info("pushHistory " + value);
//        }
    }

    public void pushQuote(String key, String value, long delayMs) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        quoteRing
                .computeIfAbsent(key, k -> new FixedRingBuffer<>(RING_SIZE))
                .offer(raw);

        totalReceived.incrementAndGet();
        quoteRegistry.submit(key, delayMs);   // <-- worker riêng cho từng symbol
    }

    // -------------------- STATS ----------------------------

    private void recordProcessingTime(long startNano) {
        long durationMs = (System.nanoTime() - startNano) / 1_000_000;
        totalProcessed.incrementAndGet();
        totalProcessingTimeMs.addAndGet(durationMs);
    }

    @Scheduled(fixedDelay = 60000)
    public void logStats() {
        int received = totalReceived.get();
        int processed = totalProcessed.get();
        long totalTime = totalProcessingTimeMs.get();
        double avgTime = processed > 0 ? (double) totalTime / processed : 0.0;

        log.info("[MapSize] historicalMap={}, ringHistory={}, quoteMap={}, ringQuote={}",
                historicalMap.size(), historicalRing.size(),
                quoteMap.size(), quoteRing.size());

        log.info("📊 Stats: received={}, processed={}, totalTime={} ms, avgTime={} ms/msg",
                received, processed, totalTime, String.format("%.2f", avgTime));

        if (historicalRing.isEmpty() && quoteRing.isEmpty()) {
            log.info("[Result] historicalMap={}, historicalMapValues {}, quoteMap={}",
                    historicalMap.size(), historicalMap.size(), quoteMap.size());
        }
    }
}