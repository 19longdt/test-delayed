package local.demo.thread_delay;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

@Slf4j
@Component
public class ZmqMessageSubscriber {

    private final DelayedSymbolCacheAdapter cacheAdapter;
    private ZContext context;
    private ZMQ.Socket subscriber;
    private Thread thread;
    private volatile boolean running = true;

    public ZmqMessageSubscriber(DelayedSymbolCacheAdapter cacheAdapter) {
        this.cacheAdapter = cacheAdapter;
    }

    @PostConstruct
    public void start() {
        context = new ZContext();
        subscriber = context.createSocket(ZMQ.SUB);
        subscriber.connect("tcp://127.0.0.1:5555");
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL); // Nhận tất cả topic

        thread = new Thread(this::listenLoop, "ZmqSubscriberThread");
        thread.start();
        log.info("[ZMQ] Subscriber started and connected to tcp://127.0.0.1:5555");
    }

    private void listenLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            ZMsg msg = ZMsg.recvMsg(subscriber, ZMQ.DONTWAIT);
            if (msg != null) {
                try {
                    String topic = msg.popString();
                    String symbol = msg.popString();
                    String body = msg.popString();

//                    log.info("[ZMQ] Received topic={} symbol={} body={}", topic, symbol, body);

                    switch (topic) {
                        case "quoteAll" -> cacheAdapter.pushQuote(symbol, body, 20000);
                        case "history" -> cacheAdapter.pushHistory(symbol, body, 20000);
                        default -> log.warn("[ZMQ] Unknown topic: {}", topic);
                    }

                } catch (Exception e) {
                    log.error("[ZMQ] Error handling message", e);
                } finally {
                    msg.destroy();
                }
            } else {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (thread != null) thread.interrupt();
        if (subscriber != null) subscriber.close();
        if (context != null) context.close();
        log.info("[ZMQ] Subscriber stopped");
    }
}
