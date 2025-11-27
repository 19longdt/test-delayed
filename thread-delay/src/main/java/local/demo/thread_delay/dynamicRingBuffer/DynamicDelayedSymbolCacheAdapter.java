package local.demo.thread_delay.dynamicRingBuffer;

import jakarta.annotation.PostConstruct;
import local.demo.thread_delay.DelayWorkerManager;
import local.demo.thread_delay.DelayedEntry;
import local.demo.thread_delay.MemoryGuardian;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DynamicDelayedSymbolCacheAdapter {

    private final DelayWorkerManager mainWorker;
    private final DelayWorkerManager quoteWorker;

    private final Map<String, String> historicalMap = new ConcurrentHashMap<>();
    private final Map<String, DynamicRingBuffer<byte[]>> historicalRing = new ConcurrentHashMap<>();

    private final Map<String, String> quoteMap = new ConcurrentHashMap<>();
    private final Map<String, DynamicRingBuffer<byte[]>> quoteRing = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    private final MemoryGuardian guardian;

    private static final int INITIAL_RING_SIZE = 100_000;
    private static final int MAX_RING_SIZE = 1_000_000; // Giới hạn tối đa

    public DynamicDelayedSymbolCacheAdapter(MemoryGuardian guardian) {
        this.guardian = guardian;
        this.mainWorker = new DelayWorkerManager("MainWorker", 5000, this::processMain);
        this.quoteWorker = new DelayWorkerManager("QuoteWorker", 5000, this::processQuote);
    }

    @PostConstruct
    public void register() {
        guardian.registerManagers(List.of(mainWorker, quoteWorker));
    }

    // -------------------- PROCESS MAIN ----------------------------

    public void processMain(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            DynamicRingBuffer<byte[]> rb = historicalRing.get(entry.getKey());
            if (rb != null) {
                byte[] val = rb.poll();
                if (val != null) {
                    historicalMap.put(entry.getKey(), new String(val, StandardCharsets.UTF_8));
                }
                if (rb.isEmpty()) {
                    historicalRing.remove(entry.getKey());
                }
            }
        } finally {
            recordProcessingTime(start);
        }
    }

    // -------------------- PROCESS QUOTE ----------------------------

    public void processQuote(DelayedEntry entry) {
        long start = System.nanoTime();
        try {
            DynamicRingBuffer<byte[]> rb = quoteRing.get(entry.getKey());
            if (rb != null) {
                byte[] val = rb.poll();
                if (val != null) {
                    quoteMap.put(entry.getKey(), new String(val, StandardCharsets.UTF_8));
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
                .computeIfAbsent(key, k -> new DynamicRingBuffer<>(INITIAL_RING_SIZE, MAX_RING_SIZE))
                .offer(raw);

        totalReceived.incrementAndGet();
        mainWorker.submit(key, delayMs);
    }

    public void pushQuote(String key, String value, long delayMs) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        quoteRing
                .computeIfAbsent(key, k -> new DynamicRingBuffer<>(INITIAL_RING_SIZE, MAX_RING_SIZE))
                .offer(raw);

        totalReceived.incrementAndGet();
        quoteWorker.submit(key, delayMs);
    }

    // -------------------- STATS ----------------------------

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

        // Tính tổng capacity và size của các buffer
        long historicalTotalCapacity = historicalRing.values().stream().mapToLong(DynamicRingBuffer::getCapacity).sum();
        long historicalTotalSize = historicalRing.values().stream().mapToLong(DynamicRingBuffer::size).sum();
        long quoteTotalCapacity = quoteRing.values().stream().mapToLong(DynamicRingBuffer::getCapacity).sum();
        long quoteTotalSize = quoteRing.values().stream().mapToLong(DynamicRingBuffer::size).sum();

        log.info("[MapSize] historicalMap={}, ringHistory={}(cap:{}/size:{}), quoteMap={}, ringQuote={}(cap:{}/size:{})",
                historicalMap.size(), historicalRing.size(), historicalTotalCapacity, historicalTotalSize,
                quoteMap.size(), quoteRing.size(), quoteTotalCapacity, quoteTotalSize);

        log.info("📊 Stats: received={}, processed={}, totalTime={} ms, avgTime={} ms/msg",
                received, processed, totalTime, String.format("%.2f", avgTime));

        // Cleanup khi buffer trống
        if (historicalRing.isEmpty() && quoteRing.isEmpty()) {
            log.info("[Result] historicalMap={}, historicalMapValues {}, quoteMap={}", historicalMap.size(), historicalMap.values().size(), quoteMap.size());
        }
    }

    // Method để manual cleanup (nếu cần)
    public void cleanupBuffer(String key) {
        DynamicRingBuffer<byte[]> historicalBuffer = historicalRing.remove(key);
        DynamicRingBuffer<byte[]> quoteBuffer = quoteRing.remove(key);
        if (historicalBuffer != null) {
            historicalBuffer.clear();
        }
        if (quoteBuffer != null) {
            quoteBuffer.clear();
        }
        historicalMap.remove(key);
        quoteMap.remove(key);
    }
}