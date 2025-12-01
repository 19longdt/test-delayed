package local.demo.thread_delay.monitor;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkerMonitor {
    public static final AtomicInteger ACTIVE_WORKERS = new AtomicInteger(0);
    public static final AtomicInteger RECEIVE_SYM1 = new AtomicInteger(0);
    public static final AtomicInteger HANDLE_SYM1 = new AtomicInteger(0);
    public static final AtomicInteger HANDLED_SYM1 = new AtomicInteger(0);
}
