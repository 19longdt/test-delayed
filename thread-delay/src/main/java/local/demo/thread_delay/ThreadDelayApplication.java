package local.demo.thread_delay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ThreadDelayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ThreadDelayApplication.class, args);
    }
}
