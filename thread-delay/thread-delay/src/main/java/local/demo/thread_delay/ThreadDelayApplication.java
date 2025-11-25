package local.demo.thread_delay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ThreadDelayApplication {

	public static void main(String[] args) {
		SpringApplication.run(ThreadDelayApplication.class, args);


        ZmqMessageReceiver receiver = new ZmqMessageReceiver("tcp://127.0.0.1:5555", msg -> {
            String topic = msg.popString();
            String symbol = msg.popString();
            String body   = msg.popString();
            System.out.printf("[ZMQ] Received topic=%s symbol=%s msg=%s%n", topic, symbol, body);
        });

        receiver.start();

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        receiver.stop();
	}

}
