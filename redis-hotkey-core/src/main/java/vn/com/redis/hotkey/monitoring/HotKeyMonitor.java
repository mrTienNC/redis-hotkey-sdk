package vn.com.redis.hotkey.monitoring;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import vn.com.redis.hotkey.monitoring.export.ExporterFactory;
import vn.com.redis.hotkey.monitoring.export.HotKeyExporter;

/**
 * Main entry point of the SDK: creates the tracker and aggregator and enables exporters by the names given in the configuration.
 * <pre>
 * HotKeyMonitor m = HotKeyMonitor.start(new HotKeyConfig().exporters("prometheus"));
 * </pre>
 */
public final class HotKeyMonitor implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(HotKeyMonitor.class.getName());

    private static volatile HotKeyMonitor global;

    private final HotKeyConfig config;
    private final HotKeyTracker tracker;
    private final Aggregator aggregator;
    private final Thread aggregatorThread;
    private final ThreadPoolExecutor exportExecutor;
    private final List<HotKeyExporter> exporters = new ArrayList<HotKeyExporter>();
    private volatile Snapshot latest;

    /** The active monitor of this JVM (null if not started). Adapters use it to find the tracker. */
    public static HotKeyMonitor global() {
        return global;
    }

    /** Creates, starts and registers the global monitor. */
    public static synchronized HotKeyMonitor start(HotKeyConfig config) {
        HotKeyMonitor old = global;
        if (old != null) {
            LOG.warning("redis-hotkey: a monitor is already running, closing the old one before creating a new one");
            old.close();
        }
        HotKeyMonitor m = new HotKeyMonitor(config);
        global = m;
        return m;
    }

    private HotKeyMonitor(HotKeyConfig config) {
        this.config = config;
        this.tracker = new HotKeyTracker(config.getQueueCapacity(), config.getSampleRate(), config.isEnabled());
        this.exportExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(2), daemon("redis-hotkey-export"),
                new ThreadPoolExecutor.DiscardOldestPolicy());

        for (String name : config.getExporters()) {
            try {
                HotKeyExporter e = ExporterFactory.create(name, config);
                e.start();
                exporters.add(e);
                LOG.info("redis-hotkey: exporter '" + e.name() + "' enabled");
            } catch (Throwable t) {
                // a failing exporter must not break the application
                LOG.log(Level.WARNING, "redis-hotkey: could not enable exporter '" + name + "'", t);
            }
        }

        this.aggregator = new Aggregator(config, tracker, new Aggregator.SnapshotListener() {
            @Override
            public void onSnapshot(final Snapshot snapshot) {
                latest = snapshot;
                exportExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (HotKeyExporter e : exporters) {
                            try {
                                e.export(snapshot);
                            } catch (Throwable t) {
                                LOG.log(Level.FINE, "redis-hotkey: exporter '" + e.name() + "' failed", t);
                            }
                        }
                    }
                });
            }
        });
        this.aggregatorThread = daemon("redis-hotkey-aggregator").newThread(aggregator);
        this.aggregatorThread.start();
        LOG.info("redis-hotkey: started (app=" + config.getAppName() + ", sampleRate=" + config.getSampleRate()
                + ", window=" + config.getWindowSeconds() + "s, exporters=" + config.getExporters() + ")");
    }

    private static ThreadFactory daemon(final String name) {
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            }
        };
    }

    public HotKeyTracker getTracker() {
        return tracker;
    }

    public HotKeyConfig getConfig() {
        return config;
    }

    /** Latest snapshot (null before the first publish cycle). */
    public Snapshot getLatestSnapshot() {
        return latest;
    }

    /** Forces a snapshot to be built now (useful for tests and diagnostic endpoints). */
    public Snapshot snapshotNow() {
        return aggregator.publish(System.currentTimeMillis());
    }

    @Override
    public synchronized void close() {
        aggregator.stop();
        aggregatorThread.interrupt();
        exportExecutor.shutdown();
        try {
            exportExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        for (HotKeyExporter e : exporters) {
            try {
                e.close();
            } catch (Throwable ignore) {
                // shutting down
            }
        }
        if (global == this) {
            global = null;
        }
    }
}
