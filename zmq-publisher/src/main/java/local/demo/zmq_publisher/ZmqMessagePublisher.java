package local.demo.zmq_publisher;

import lombok.extern.log4j.Log4j2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Random;
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

            while (running.get() && sent < totalMessages) {

                // ✅ Gửi 100 msg/lần
                for (int i = 0; i < 100 && sent < totalMessages; i++) {

                    String topic = (i < 80) ? "history" : "quoteAll"; // 90/10 ratio
                    String symbol = "SYM" + rand.nextInt(1000);
                    String message = i + "______{\"web-app\":{\"servlet\":[{\"servlet-name\":\"cofaxCDS\",\"servlet-class\":\"org.cofax.cds.CDSServlet\",\"init-param\":{\"configGlossary:installationAt\":\"Philadelphia, PA\",\"configGlossary:adminEmail\":\"ksm@pobox.com\",\"configGlossary:poweredBy\":\"Cofax\",\"configGlossary:poweredByIcon\":\"/images/cofax.gif\",\"configGlossary:staticPath\":\"/content/static\",\"templateProcessorClass\":\"org.cofax.WysiwygTemplate\",\"templateLoaderClass\":\"org.cofax.FilesTemplateLoader\",\"templatePath\":\"templates\",\"templateOverridePath\":\"\",\"defaultListTemplate\":\"listTemplate.htm\",\"defaultFileTemplate\":\"articleTemplate.htm\",\"useJSP\":false,\"jspListTemplate\":\"listTemplate.jsp\",\"jspFileTemplate\":\"articleTemplate.jsp\",\"cachePackageTagsTrack\":200,\"cachePackageTagsStore\":200,\"cachePackageTagsRefresh\":60,\"cacheTemplatesTrack\":100,\"cacheTemplatesStore\":50,\"cacheTemplatesRefresh\":15,\"cachePagesTrack\":200,\"cachePagesStore\":100,\"cachePagesRefresh\":10,\"cachePagesDirtyRead\":10,\"searchEngineListTemplate\":\"forSearchEnginesList.htm\",\"searchEngineFileTemplate\":\"forSearchEngines.htm\",\"searchEngineRobotsDb\":\"WEB-INF/robots.db\",\"useDataStore\":true,\"dataStoreClass\":\"org.cofax.SqlDataStore\",\"redirectionClass\":\"org.cofax.SqlRedirection\",\"dataStoreName\":\"cofax\",\"dataStoreDriver\":\"com.microsoft.jdbc.sqlserver.SQLServerDriver\",\"dataStoreUrl\":\"jdbc:microsoft:sqlserver://LOCALHOST:1433;DatabaseName=goon\",\"dataStoreUser\":\"sa\",\"dataStorePassword\":\"dataStoreTestQuery\",\"dataStoreTestQuery\":\"SET NOCOUNT ON;select test='test';\",\"dataStoreLogFile\":\"/usr/local/tomcat/logs/datastore.log\",\"dataStoreInitConns\":10,\"dataStoreMaxConns\":100,\"dataStoreConnUsageLimit\":100,\"dataStoreLogLevel\":\"debug\",\"maxUrlLength\":500}},{\"servlet-name\":\"cofaxEmail\",\"servlet-class\":\"org.cofax.cds.EmailServlet\",\"init-param\":{\"mailHost\":\"mail1\",\"mailHostOverride\":\"mail2\"}},{\"servlet-name\":\"cofaxAdmin\",\"servlet-class\":\"org.cofax.cds.AdminServalet\"},{\"servlet-name\":\"fileServlet\",\"servlet-class\":\"org.cofax.cds.FileServlet\"},{\"servlet-name\":\"cofaxTools\",\"servlet-class\":\"org.cofax.cms.CofaxToolsServlet\",\"init-param\":{\"templatePath\":\"toolstemplates/\",\"log\":1,\"logLocation\":\"/usr/local/tomcat/logs/CofaxTools.log\",\"logMaxSize\":\"\",\"dataLog\":1,\"dataLogLocation\":\"/usr/local/tomcat/logs/dataLog.log\",\"dataLogMaxSize\":\"\",\"removePageCache\":\"/content/admin/remove?cache=pages&id=\",\"removeTemplateCache\":\"/content/admin/remove?cache=templates&id=\",\"fileTransferFolder\":\"/usr/local/tomcat/webapps/content/fileTransferFolder\",\"lookInContext\":1,\"adminGroupID\":4,\"betaServer\":true}}],\"servlet-mapping\":{\"cofaxCDS\":\"/\",\"cofaxEmail\":\"/cofaxutil/aemail/*\",\"cofaxAdmin\":\"/admin/*\",\"fileServlet\":\"/static/*\",\"cofaxTools\":\"/tools/*\"},\"taglib\":{\"taglib-uri\":\"cofax.tld\",\"taglib-location\":\"/WEB-INF/tlds/cofax.tld\"}}}";

                    // Gửi 3 frame: topic, symbol, message
                    publisher.sendMore(topic);
                    publisher.sendMore(symbol);
                    publisher.send(message);

                    sent++;
                }

                Thread.sleep(1);

                if (sent % 10000 == 0) {
                    log.info("[ZMQ] Progress: sent {} messages so far...", sent);
                }
            }

            long elapsed = System.currentTimeMillis() - start;
            double rate = (sent * 1000.0) / elapsed;
            log.info("[ZMQ] Finished sending {} messages in {}s (~{} msg/s)",
                    sent, String.format("%.2f", elapsed / 1000.0), String.format("%.0f", rate));

        } catch (Exception e) {
            log.error("[ZMQ] Error while publishing", e);
        } finally {
            running.set(false);
        }
    }
}
