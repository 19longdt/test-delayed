package local.demo.thread_delay;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerConfig {

    @Bean("mainWorker")
    public DelayWorkerManager mainWorker(DelayedSymbolCacheAdapter adapter) {
        return new DelayWorkerManager("MainWorker", 5000, adapter::processMain);
    }

    @Bean("quoteWorker")
    public DelayWorkerManager quoteWorker(DelayedSymbolCacheAdapter adapter) {
        return new DelayWorkerManager("QuoteWorker", 5000, adapter::processQuote);
    }
}