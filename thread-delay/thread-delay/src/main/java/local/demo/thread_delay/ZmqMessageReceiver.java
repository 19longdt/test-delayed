package local.demo.thread_delay;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class chuyên nhận message từ ZeroMQ, hỗ trợ start/stop an toàn.
 * Có thể inject callback để xử lý mỗi khi nhận được msg mới.
 */
public class ZmqMessageReceiver implements Runnable {

    private final String endpoint;                  // Ví dụ: "tcp://127.0.0.1:5555"
    private final java.util.function.Consumer<ZMsg> messageHandler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread thread;
    private ZContext context;
    private ZMQ.Socket subscriber;

    public ZmqMessageReceiver(String endpoint, java.util.function.Consumer<ZMsg> handler) {
        this.endpoint = endpoint;
        this.messageHandler = handler;
    }

    /** Khởi động thread nhận ZMQ message */
    public synchronized void start() {
        if (running.get()) return;

        running.set(true);
        context = new ZContext();
        subscriber = context.createSocket(ZMQ.SUB);
        subscriber.connect(endpoint);
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);  // nhận tất cả topic

        thread = new Thread(this, "ZmqReceiverThread");
        thread.setDaemon(true);
        thread.start();
        System.out.printf("[ZMQ] Receiver started at %s%n", endpoint);
    }

    /** Dừng thread & đóng socket */
    public synchronized void stop() {
        running.set(false);
        if (thread != null) thread.interrupt();

        if (subscriber != null) subscriber.close();
        if (context != null) context.close();

        System.out.println("[ZMQ] Receiver stopped.");
    }

    @Override
    public void run() {
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                // Chờ message mới
                ZMsg msg = ZMsg.recvMsg(subscriber, ZMQ.DONTWAIT);
                if (msg != null) {
                    messageHandler.accept(msg); // gọi callback xử lý
                } else {
                    Thread.sleep(10); // tránh busy loop
                }
            }
        } catch (InterruptedException ignored) {
        } finally {
            stop();
        }
    }
}