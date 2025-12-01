package local.demo.zmq_publisher;

import lombok.extern.log4j.Log4j2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Publisher ZMQ, có thể gửi hàng triệu message để test throughput.
 * Mặc định dùng mô hình PUB/SUB, gửi ngẫu nhiên topic và symbol.
 */
@Log4j2
public class ZmqMessagePublisher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ZmqMessagePublisher.class);
    private final String endpoint; // ví dụ: "tcp://*:5555"
    private final int totalMessages;
    private Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ZmqMessagePublisher(String endpoint, int totalMessages) {
        this.endpoint = endpoint;
        this.totalMessages = totalMessages;
    }

    /**
     * Khởi động thread gửi message
     */
    public synchronized void start() {
        if (running.get()) return;
        running.set(true);
        thread = new Thread(this, "ZmqPublisherThread");
//        thread.setDaemon(true);
        thread.start();
        log.info("[ZMQ] Publisher started at {}", endpoint);
    }

    /**
     * Dừng thread gửi message
     */
    public synchronized void stop() {
        running.set(false);
        if (thread != null) thread.interrupt();
        log.info("[ZMQ] Publisher stopped.");
    }

    public void join() throws InterruptedException {
        if (thread != null) thread.join();
    }

    @Override
    public void run() {
        try (ZContext context = new ZContext();
             ZMQ.Socket publisher = context.createSocket(ZMQ.PUB)) {

            publisher.bind(endpoint);
            Random rand = new Random();

            long start = System.currentTimeMillis();
            int sent = 0;

            // NEW: Sequence per symbol
            Map<String, Long> symbolSeq = new ConcurrentHashMap<>();

            while (running.get() && sent < totalMessages) {

                for (int i = 0; i < 100 && sent < totalMessages; i++) {

                    String topic = "history";

                    // Random symbol
                    String symbol = "SYM" + rand.nextInt(20);

                    // --- symbol sequence (đảm bảo tuần tự theo symbol) ---
                    long sSeq = symbolSeq.getOrDefault(symbol, 0L);
                    symbolSeq.put(symbol, sSeq + 1);

                    // Message payload
                    String body = " | SYM=" + symbol + " | SYM_SEQ=" + sSeq +
                            " | MSG=" + i +
                            " | PAYLOAD={...big_json...}";
                    if (symbol.equals("SYM1")) {
                        log.info(body);
                    }

                    // ZMQ multipart
                    publisher.sendMore(topic);
                    publisher.sendMore(symbol);
                    publisher.send(body);

                    sent++;
                }

                Thread.sleep(5);

                if (sent % 5000 == 0) {
                    log.info("[ZMQ] Progress: sent {} messages...", sent);
                }
            }

            long elapsed = System.currentTimeMillis() - start;
            double rate = (sent * 1000.0) / elapsed;

            log.info("[ZMQ] Finished sending {} messages in {}s (~{} msg/s)",
                    sent,
                    String.format("%.2f", elapsed / 1000.0),
                    String.format("%.0f", rate));

        } catch (Exception e) {
            log.error("[ZMQ] Error while publishing", e);
        } finally {
            running.set(false);
        }
    }

}
