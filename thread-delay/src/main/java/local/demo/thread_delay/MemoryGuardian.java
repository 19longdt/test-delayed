package local.demo.thread_delay;

import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Log4j2
@Component
public class MemoryGuardian {

    private final List<DelayWorkerManager> managers = new CopyOnWriteArrayList<>();

    public void registerManagers(List<DelayWorkerManager> list) {
        managers.addAll(list);
    }

    /**
     * Kiểm tra định kỳ mỗi 20 giây
     */
//    @Scheduled(fixedDelay = 20_000) // sau mỗi 60s, khi lần trước chạy xong
    public void checkWorkersAndMemory() {
        if (managers.isEmpty()) {
            log.warn("[MemoryGuardian] No DelayWorkerManagers registered.");
            return;
        }
        boolean allIdle = managers.stream().noneMatch(DelayWorkerManager::isRunning);

        long usedMb = usedMemoryMb();
        long totalMb = totalMemoryMb();

        if (allIdle) {
            log.info("[MemoryGuardian] All DelayWorkers are idle. Used: {}/{} MB. Triggering GC...", usedMb, totalMb);
            System.gc();
            sleep(1000);
            long after = usedMemoryMb();
            log.info("[MemoryGuardian] GC done. Before: {} MB → After: {} MB", usedMb, after);
        } else {
            log.debug("[MemoryGuardian] Workers active. Used: {}/{} MB", usedMb, totalMb);
        }
    }

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
