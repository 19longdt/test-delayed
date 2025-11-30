package local.demo.thread_delay.monitor;

import local.demo.thread_delay.DelayWorkersRegistry;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Log4j2
@Component
public class MemoryGuardian {

    private final List<DelayWorkersRegistry> managers = new CopyOnWriteArrayList<>();

    public void registerManagers(List<DelayWorkersRegistry> list) {
        managers.addAll(list);
    }

    /**
     * Kiểm tra định kỳ mỗi 20 giây
     */
//    @Scheduled(fixedDelay = 20_000) // sau mỗi 60s, khi lần trước chạy xong
//    public void checkWorkersAndMemory() {
//        if (managers.isEmpty()) {
//            log.warn("[MemoryGuardian] No DelayWorkersRegistry registered.");
//            return;
//        }
//        boolean allIdle = managers.stream().noneMatch(DelayWorkersRegistry::isRunning);
//
//        long usedMb = usedMemoryMb();
//        long totalMb = totalMemoryMb();
//
//        if (allIdle) {
//            log.info("[MemoryGuardian] All DelayWorkers are idle. Used: {}/{} MB. Triggering GC...", usedMb, totalMb);
//            System.gc();
//            sleep(1000);
//            long after = usedMemoryMb();
//            log.info("[MemoryGuardian] GC done. Before: {} MB → After: {} MB", usedMb, after);
//        } else {
//            log.debug("[MemoryGuardian] Workers active. Used: {}/{} MB", usedMb, totalMb);
//        }
//    }

    private long usedMemoryMb() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    }

    private long totalMemoryMb() {
        return Runtime.getRuntime().totalMemory() / 1024 / 1024;
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
