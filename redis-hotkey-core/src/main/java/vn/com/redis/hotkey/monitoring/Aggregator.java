package vn.com.redis.hotkey.monitoring;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import vn.com.redis.hotkey.monitoring.HotKeyTracker.Sample;

/** Single daemon thread that processes samples: normalizes keys, counts per window, and periodically builds a {@link Snapshot}. */
final class Aggregator implements Runnable {

    private static final Logger LOG = Logger.getLogger(Aggregator.class.getName());
    private static final int MAX_DRAIN_PER_LOOP = 5000;

    interface SnapshotListener {
        void onSnapshot(Snapshot snapshot);
    }

    private final HotKeyConfig config;
    private final HotKeyTracker tracker;
    private final KeyNormalizer normalizer;
    private final SlidingWindow window;
    private final SnapshotListener listener;
    private final long startedAtMs = System.currentTimeMillis();
    private volatile boolean running = true;
    private long lastErrorLogMs;

    Aggregator(HotKeyConfig config, HotKeyTracker tracker, SnapshotListener listener) {
        this.config = config;
        this.tracker = tracker;
        this.listener = listener;
        this.normalizer = new KeyNormalizer(config.getNormalizerRules(), config.getMaxKeyLength());
        this.window = new SlidingWindow(config.getWindowSeconds(), config.getBucketSeconds(),
                config.getPatternCapacity(), config.isExportRawKeys() ? config.getRawKeyCapacity() : 0);
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {
        long publishMs = Math.max(1, config.getPublishIntervalSeconds()) * 1000L;
        long nextPublish = System.currentTimeMillis() + publishMs;
        while (running) {
            try {
                Sample s = tracker.queue().poll(200, TimeUnit.MILLISECONDS);
                long now = System.currentTimeMillis();
                int drained = 0;
                while (s != null && drained < MAX_DRAIN_PER_LOOP) {
                    handle(s, now);
                    drained++;
                    s = tracker.queue().poll();
                }
                if (now >= nextPublish) {
                    publish(now);
                    nextPublish = now + publishMs;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                logRateLimited(t);
            }
        }
    }

    private void handle(Sample s, long now) {
        String pattern = normalizer.normalize(s.key);
        String raw = config.isExportRawKeys()
                ? (s.key.length() > config.getMaxKeyLength() ? s.key.substring(0, config.getMaxKeyLength()) : s.key)
                : null;
        window.add(now, pattern, raw, s.write, s.weight);
    }

    Snapshot publish(long now) {
        SlidingWindow.Merged m = window.merge(now);
        double seconds = Math.max(1.0d, Math.min(window.windowMs(), now - startedAtMs) / 1000.0d);
        int topN = Math.max(1, config.getTopN());
        List<Snapshot.Entry> patterns = SlidingWindow.topPatterns(m.patterns, topN, seconds);
        List<Snapshot.Entry> raw = config.isExportRawKeys()
                ? SlidingWindow.topRawKeys(m.rawKeys, topN, seconds)
                : java.util.Collections.<Snapshot.Entry>emptyList();
        Snapshot snap = new Snapshot(now, config.getAppName(), config.getInstanceId(),
                (int) (window.windowMs() / 1000L), tracker.getSampleRate(), tracker.isEnabled(),
                tracker.getDropped(), tracker.getQueueSize(), tracker.getCommandsSeen(), tracker.getParseErrors(),
                m.reads / seconds, m.writes / seconds, tracker.dangerousCommands(), patterns, raw);
        listener.onSnapshot(snap);
        return snap;
    }

    /** For tests: processes one sample synchronously. */
    void handleForTest(Sample s, long now) {
        handle(s, now);
    }

    private void logRateLimited(Throwable t) {
        long now = System.currentTimeMillis();
        if (now - lastErrorLogMs > 60000L) {
            lastErrorLogMs = now;
            LOG.log(Level.WARNING, "redis-hotkey aggregator error (will keep running)", t);
        }
    }
}
