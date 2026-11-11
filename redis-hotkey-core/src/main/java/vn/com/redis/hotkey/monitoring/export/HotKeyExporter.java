package vn.com.redis.hotkey.monitoring.export;

import vn.com.redis.hotkey.monitoring.Snapshot;

/**
 * Destination for exported data. A new exporter can be added by implementing this interface and registering it via
 * {@code META-INF/services/vn.com.redis.hotkey.monitoring.export.HotKeyExporterProvider}.
 */
public interface HotKeyExporter {

    String name();

    /** Opens resources (HTTP port, file...). If it throws, the exporter is skipped and the application keeps running. */
    void start() throws Exception;

    /** Called from a dedicated thread on every publish cycle. Must have a timeout; errors are only logged. */
    void export(Snapshot snapshot) throws Exception;

    void close();
}
