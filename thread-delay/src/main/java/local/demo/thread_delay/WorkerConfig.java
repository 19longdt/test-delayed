package local.demo.thread_delay;

import local.demo.thread_delay.dynamicRingBuffer.DynamicDelayedSymbolCacheAdapter;
import local.demo.thread_delay.ringBuffer.FixDelayedSymbolCacheAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerConfig {

    @Bean("mainWorker")
    public DelayWorkerManager mainWorker(FixDelayedSymbolCacheAdapter adapter) {
        return new DelayWorkerManager("MainWorker", 5000, adapter::processMain);
    }

    @Bean("quoteWorker")
    public DelayWorkerManager quoteWorker(FixDelayedSymbolCacheAdapter adapter) {
        return new DelayWorkerManager("QuoteWorker", 5000, adapter::processQuote);
    }
}