package local.demo.thread_delay;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedEntry implements Delayed {
    private final String key;
    private final long expireAt;

    public DelayedEntry(String key, long delayMs) {
        this.key = key;
        this.expireAt = System.currentTimeMillis() + delayMs;
    }

    public String getKey() {
        return key;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = expireAt - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS),
                o.getDelay(TimeUnit.MILLISECONDS));
    }
}
