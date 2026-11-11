package vn.com.redis.hotkey.monitoring.export;

import java.util.Locale;
import java.util.ServiceLoader;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;

/** Creates an exporter by the NAME declared in the configuration. Names are case-insensitive. */
public final class ExporterFactory {

    private ExporterFactory() {
    }

    /**
     * @param name "prometheus" | "td-agent" (alias: tdagent, td_agent, fluentd) | a name provided by an SPI
     * @throws IllegalArgumentException if the name is not recognized
     */
    public static HotKeyExporter create(String name, HotKeyConfig config) {
        String n = name == null ? "" : name.trim().toLowerCase(Locale.ENGLISH);
        if ("prometheus".equals(n)) {
            return new PrometheusExporter(config);
        }
        if ("td-agent".equals(n) || "tdagent".equals(n) || "fluentd".equals(n) || "td_agent".equals(n)) {
            return new TdAgentExporter(config);
        }
        for (HotKeyExporterProvider p : ServiceLoader.load(HotKeyExporterProvider.class)) {
            if (p.supports(n)) {
                return p.create(config);
            }
        }
        throw new IllegalArgumentException("Unsupported exporter: '" + name + "'. Use: prometheus, td-agent");
    }
}
