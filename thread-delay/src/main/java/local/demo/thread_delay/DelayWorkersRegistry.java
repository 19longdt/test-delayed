package local.demo.thread_delay;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class DelayWorkersRegistry {

    private final ConcurrentHashMap<String, DelayWorkerManager> map = new ConcurrentHashMap<>();
    private final long idleTimeoutMs;
    private final Consumer<DelayedEntry> handler;

    public DelayWorkersRegistry(long idleTimeoutMs,
                                Consumer<DelayedEntry> handler) {
        this.idleTimeoutMs = idleTimeoutMs;
        this.handler = handler;
    }

    public void submit(String symbol, long delayMs) {
        DelayWorkerManager m = map.computeIfAbsent(symbol,
                s -> new DelayWorkerManager(s, idleTimeoutMs, handler));

        m.submit(delayMs);

        if (!m.isRunning()) {
            map.remove(symbol, m);
        }
    }
}
