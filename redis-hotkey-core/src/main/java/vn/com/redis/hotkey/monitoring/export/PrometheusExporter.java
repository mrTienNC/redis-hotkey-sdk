package vn.com.redis.hotkey.monitoring.export;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.Snapshot;

/**
 * Prometheus (pull): opens a small HTTP server (JDK {@code com.sun.net.httpserver}, available since Java 6) that
 * returns the latest snapshot at {@code /metrics}. No external library needed.
 */
public final class PrometheusExporter implements HotKeyExporter {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final HotKeyConfig config;
    private volatile Snapshot latest;
    private HttpServer server;

    public PrometheusExporter(HotKeyConfig config) {
        this.config = config;
    }

    @Override
    public String name() {
        return "prometheus";
    }

    @Override
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(config.getPrometheusBindAddress(), config.getPrometheusPort()), 8);
        server.createContext(config.getPrometheusPath(), new HttpHandler() {
            @Override
            public void handle(HttpExchange ex) throws IOException {
                try {
                    Snapshot s = latest;
                    byte[] body = (s == null ? "# no snapshot yet\n" : PrometheusTextFormat.render(s)).getBytes(UTF8);
                    ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                    ex.sendResponseHeaders(200, body.length);
                    OutputStream os = ex.getResponseBody();
                    try {
                        os.write(body);
                    } finally {
                        os.close();
                    }
                } finally {
                    ex.close();
                }
            }
        });
        server.setExecutor(Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "redis-hotkey-prometheus");
                t.setDaemon(true);
                return t;
            }
        }));
        server.start();
    }

    @Override
    public void export(Snapshot snapshot) {
        this.latest = snapshot;
    }

    /** Port being listened on (useful when the port is configured as 0 in tests). */
    public int getPort() {
        return server == null ? -1 : server.getAddress().getPort();
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }
}
