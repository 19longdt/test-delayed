package local.demo.thread_delay.ringBuffer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class FixedRingBuffer<T> {

    private final byte[][] buffer;
    private final int mask;
    private volatile long head = 0;
    private volatile long tail = 0;

    public FixedRingBuffer(int sizePowerOfTwo) {
        this.buffer = new byte[sizePowerOfTwo][];
        this.mask = sizePowerOfTwo - 1;
    }

    public synchronized void offer(byte[] value) {
        long t = tail;
        long h = head;
        if (t - h == buffer.length) {
            log.warn("Full buffer size: " + buffer.length + " head: " + head + " tail: " + tail);
            return; // full
        }
        buffer[(int)(t & mask)] = value;
        tail = t + 1;
    }

    public synchronized byte[] poll() {
        long h = head;
        if (h == tail) return null;
        int idx = (int)(h & mask);
        byte[] v = buffer[idx];
        buffer[idx] = null;
        head = h + 1;
        return v;
    }

    public boolean isEmpty() {
        return head == tail;
    }
}
