package local.demo.zmq_publisher;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ZmqPublisherApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(ZmqPublisherApplication.class, args);
	}

    @Bean
    public CommandLineRunner runPublisher() {
        return args -> {
            ZmqMessagePublisher pub = new ZmqMessagePublisher("tcp://*:5555", 8_000_000);
            pub.start();
            pub.join(); // chờ publisher chạy xong
        };
    }
}
