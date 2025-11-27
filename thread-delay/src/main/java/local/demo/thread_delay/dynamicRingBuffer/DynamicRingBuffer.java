package local.demo.thread_delay.dynamicRingBuffer;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DynamicRingBuffer<T> {

    private byte[][] buffer;
    private final int initialCapacity;
    @Getter
    private int capacity;
    private final int maxCapacity;
    private volatile long head = 0;
    private volatile long tail = 0;
    private final Object resizeLock = new Object();

    public DynamicRingBuffer(int initialCapacity) {
        this(initialCapacity, Integer.MAX_VALUE); // Mặc định không giới hạn max
    }

    public DynamicRingBuffer(int initialCapacity, int maxCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Initial capacity must be positive");
        }
        this.initialCapacity = initialCapacity;
        this.capacity = initialCapacity;
        this.maxCapacity = maxCapacity;
        this.buffer = new byte[initialCapacity][];
    }

    public void offer(byte[] value) {
        synchronized (resizeLock) {
            long t = tail;
            long h = head;
            long size = t - h;

            // Nếu buffer đầy, thử resize
            if (size >= capacity) {
                if (capacity >= maxCapacity) {
                    log.warn("Buffer reached max capacity: {}, unable to resize. Head: {}, Tail: {}",
                            maxCapacity, head, tail);
                    return; // Đã đạt max capacity, không thể resize
                }

                int newCapacity = calculateNewCapacity();
                if (newCapacity > maxCapacity) {
                    newCapacity = maxCapacity;
                }

                if (newCapacity > capacity) {
                    resize(newCapacity);
                } else {
                    log.warn("Buffer full, unable to resize. Head: {}, Tail: {}", head, tail);
                    return;
                }
            }

            buffer[(int)(t % capacity)] = value;
            tail = t + 1;
        }
    }

    public byte[] poll() {
        synchronized (resizeLock) {
            long h = head;
            if (h == tail) return null;

            int idx = (int)(h % capacity);
            byte[] v = buffer[idx];
            buffer[idx] = null;
            head = h + 1;

            // Tự động thu nhỏ nếu buffer quá rỗng
            autoShrink();

            return v;
        }
    }

    public boolean isEmpty() {
        return head == tail;
    }

    public int size() {
        return (int)(tail - head);
    }

    private int calculateNewCapacity() {
        int newCapacity = capacity * 2;
        // Đảm bảo không vượt quá maxCapacity
        if (newCapacity > maxCapacity) {
            newCapacity = maxCapacity;
        }
        // Đảm bảo không nhỏ hơn initial capacity
        if (newCapacity < initialCapacity) {
            newCapacity = initialCapacity;
        }
        return newCapacity;
    }

    private void resize(int newCapacity) {
        log.info("Resizing buffer from {} to {}, head: {}, tail: {}",
                capacity, newCapacity, head, tail);

        byte[][] newBuffer = new byte[newCapacity][];

        long currentSize = tail - head;
        for (long i = 0; i < currentSize; i++) {
            int oldIndex = (int)((head + i) % capacity);
            int newIndex = (int)(i % newCapacity);
            newBuffer[newIndex] = buffer[oldIndex];
        }

        this.buffer = newBuffer;
        this.capacity = newCapacity;
        // Reset head và tail sau khi resize
        this.head = 0;
        this.tail = currentSize;
    }

    private void autoShrink() {
        long currentSize = tail - head;
        // Nếu size nhỏ hơn 25% capacity và capacity lớn hơn initial capacity
        if (currentSize < capacity / 4 && capacity > initialCapacity * 2) {
            int newCapacity = Math.max(initialCapacity, capacity / 2);
            log.info("Auto-shrinking buffer from {} to {}, current size: {}",
                    capacity, newCapacity, currentSize);
            resize(newCapacity);
        }
    }

    public void clear() {
        synchronized (resizeLock) {
            for (int i = 0; i < capacity; i++) {
                buffer[i] = null;
            }
            head = 0;
            tail = 0;
            // Reset về capacity ban đầu
            if (capacity != initialCapacity) {
                this.buffer = new byte[initialCapacity][];
                this.capacity = initialCapacity;
            }
        }
    }
}