package local.demo.thread_delay;

import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.*;
import java.util.List;

/**
 * Theo dõi hoạt động GC và bộ nhớ heap thực tế trong JVM.
 * Ghi log định kỳ về:
 *  - dung lượng heap hiện tại (used / max)
 *  - số lần GC đã chạy (Young / Old)
 *  - tổng thời gian GC
 */
@Log4j2
@Component
public class GCDebugHelper {

    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private long lastGcCount = 0;
    private long lastGcTime = 0;

    @PostConstruct
    public void init() {
        log.info("[GCDebugHelper] Initialized with GC beans: {}",
                gcBeans.stream().map(GarbageCollectorMXBean::getName).toList());
    }

    /**
     * Ghi log GC & heap mỗi 10 giây.
     */
    @Scheduled(fixedRate = 10_000)
    public void logMemoryAndGC() {
//        byte[][] filler = new byte[20][1024 * 1024]; // 20 MB allocation
//        filler = null;
//        MemoryUsage heap = memoryBean.getHeapMemoryUsage();
//        long used = heap.getUsed() / 1024 / 1024;
//        long committed = heap.getCommitted() / 1024 / 1024;
//        long max = heap.getMax() / 1024 / 1024;
//
//        long totalGcCount = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
//        long totalGcTime = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
//
//        long newGcCount = totalGcCount - lastGcCount;
//        long newGcTime = totalGcTime - lastGcTime;
//
//        String gcActivity = newGcCount > 0
//                ? String.format("GC activity detected: +%d runs (%.2f ms/run)", newGcCount,
//                newGcCount == 0 ? 0 : (newGcTime * 1.0 / newGcCount))
//                : "No GC activity since last check";
//
//        log.info("[GC Monitor] Heap: used={}MB / committed={}MB / max={}MB | {}",
//                used, committed, max, gcActivity);
//
//        lastGcCount = totalGcCount;
//        lastGcTime = totalGcTime;
    }

    /**
     * Gọi thử System.gc() thủ công và ghi nhận phản ứng.
     */
    public void forceGC() {
        log.warn("[GC Monitor] Manual GC triggered!");
        long before = memoryBean.getHeapMemoryUsage().getUsed() / 1024 / 1024;
        System.gc();
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        long after = memoryBean.getHeapMemoryUsage().getUsed() / 1024 / 1024;
        log.warn("[GC Monitor] Manual GC done. Heap used: {}MB → {}MB", before, after);
    }
}
