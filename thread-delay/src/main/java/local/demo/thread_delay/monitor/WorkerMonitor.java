package local.demo.thread_delay.monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkerMonitor {
    public static final AtomicInteger ACTIVE_WORKERS = new AtomicInteger(0);
}
