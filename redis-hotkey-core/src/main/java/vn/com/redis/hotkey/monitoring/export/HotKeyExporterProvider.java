package vn.com.redis.hotkey.monitoring.export;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;

/** SPI (java.util.ServiceLoader) for adding external exporters without modifying the SDK. */
public interface HotKeyExporterProvider {

    /** Name (case-insensitive) used in the configuration, e.g. "prometheus". */
    boolean supports(String name);

    HotKeyExporter create(HotKeyConfig config);
}
