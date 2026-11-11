package vn.com.redis.hotkey.monitoring;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import vn.com.redis.hotkey.monitoring.resp.CommandSink;
import vn.com.redis.hotkey.monitoring.resp.RespCommandSniffer;

/**
 * Collection point on the hot path. Principles: fast, non-blocking, never throws into the main flow.
 * When the queue is full the sample is dropped and the {@code dropped} counter is incremented.
 */
public final class HotKeyTracker implements CommandSink {

    /** A sampled key. weight = 1/sampleRate at sampling time, used to estimate the total. */
    static final class Sample {
        final String key;
        final boolean write;
        final int weight;

        Sample(String key, boolean write, int weight) {
            this.key = key;
            this.write = write;
            this.weight = weight;
        }
    }

    private final ArrayBlockingQueue<Sample> queue;
    private final LongAdder dropped = new LongAdder();
    private final LongAdder commandsSeen = new LongAdder();
    private final LongAdder parseErrors = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> dangerous = new ConcurrentHashMap<String, LongAdder>();

    private volatile boolean enabled;
    private volatile double sampleRate;
    private volatile int weight;

    public HotKeyTracker(int queueCapacity, double sampleRate, boolean enabled) {
        this.queue = new ArrayBlockingQueue<Sample>(Math.max(16, queueCapacity));
        this.enabled = enabled;
        setSampleRate(sampleRate);
    }

    @Override
    public void onCommand(String command) {
        try {
            if (!enabled) {
                return;
            }
            commandsSeen.increment();
            if (RespCommandSniffer.isDangerous(command)) {
                LongAdder a = dangerous.get(command);
                if (a == null) {
                    LongAdder created = new LongAdder();
                    a = dangerous.putIfAbsent(command, created);
                    if (a == null) {
                        a = created;
                    }
                }
                a.increment();
            }
        } catch (Throwable ignore) {
            // does not affect real Redis commands
        }
    }

    @Override
    public boolean shouldSample() {
        try {
            if (!enabled) {
                return false;
            }
            double r = sampleRate;
            return r >= 1.0d || ThreadLocalRandom.current().nextDouble() < r;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public void onKey(String command, String key, boolean write) {
        record(key, write);
    }

    /** Records one key access. Use directly when not going through the sniffer. */
    public void record(String key, boolean write) {
        try {
            if (!enabled || key == null) {
                return;
            }
            if (!queue.offer(new Sample(key, write, weight))) {
                dropped.increment();
            }
        } catch (Throwable ignore) {
            // must never affect real Redis commands
        }
    }

    @Override
    public void onParseError() {
        parseErrors.increment();
    }

    // ---- runtime controls ----

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public void setSampleRate(double rate) {
        if (!(rate > 0.0d)) {
            rate = 0.0001d;
        }
        if (rate > 1.0d) {
            rate = 1.0d;
        }
        this.sampleRate = rate;
        this.weight = (int) Math.max(1L, Math.round(1.0d / rate));
    }

    // ---- self-observation metrics ----

    public long getDropped() { return dropped.sum(); }
    public long getCommandsSeen() { return commandsSeen.sum(); }
    public long getParseErrors() { return parseErrors.sum(); }
    public int getQueueSize() { return queue.size(); }

    Map<String, Long> dangerousCommands() {
        Map<String, Long> out = new LinkedHashMap<String, Long>();
        for (Map.Entry<String, LongAdder> e : dangerous.entrySet()) {
            out.put(e.getKey(), e.getValue().sum());
        }
        return out;
    }

    ArrayBlockingQueue<Sample> queue() {
        return queue;
    }
}
