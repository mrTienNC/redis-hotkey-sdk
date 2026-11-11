package vn.com.redis.hotkey.monitoring;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable snapshot of the time window, built by the aggregator and handed to the exporters. */
public final class Snapshot {

    /** One Top-K row: a pattern (or raw key) with its estimated read/write rate per second. */
    public static final class Entry {
        private final String name;
        private final double readPerSec;
        private final double writePerSec;

        public Entry(String name, double readPerSec, double writePerSec) {
            this.name = name;
            this.readPerSec = readPerSec;
            this.writePerSec = writePerSec;
        }

        public String getName() { return name; }
        public double getReadPerSec() { return readPerSec; }
        public double getWritePerSec() { return writePerSec; }
        public double getTotalPerSec() { return readPerSec + writePerSec; }
    }

    private final long timestampMs;
    private final String appName;
    private final String instanceId;
    private final int windowSeconds;
    private final double sampleRate;
    private final boolean enabled;
    private final long dropped;
    private final int queueSize;
    private final long commandsSeen;
    private final long parseErrors;
    private final double totalReadPerSec;
    private final double totalWritePerSec;
    private final Map<String, Long> dangerousCommands;
    private final List<Entry> patterns;
    private final List<Entry> rawKeys;

    public Snapshot(long timestampMs, String appName, String instanceId, int windowSeconds, double sampleRate,
                    boolean enabled, long dropped, int queueSize, long commandsSeen, long parseErrors,
                    double totalReadPerSec, double totalWritePerSec, Map<String, Long> dangerousCommands,
                    List<Entry> patterns, List<Entry> rawKeys) {
        this.timestampMs = timestampMs;
        this.appName = appName;
        this.instanceId = instanceId;
        this.windowSeconds = windowSeconds;
        this.sampleRate = sampleRate;
        this.enabled = enabled;
        this.dropped = dropped;
        this.queueSize = queueSize;
        this.commandsSeen = commandsSeen;
        this.parseErrors = parseErrors;
        this.totalReadPerSec = totalReadPerSec;
        this.totalWritePerSec = totalWritePerSec;
        this.dangerousCommands = Collections.unmodifiableMap(new LinkedHashMap<String, Long>(dangerousCommands));
        this.patterns = Collections.unmodifiableList(patterns);
        this.rawKeys = Collections.unmodifiableList(rawKeys);
    }

    public long getTimestampMs() { return timestampMs; }
    public String getAppName() { return appName; }
    public String getInstanceId() { return instanceId; }
    public int getWindowSeconds() { return windowSeconds; }
    public double getSampleRate() { return sampleRate; }
    public boolean isEnabled() { return enabled; }
    public long getDropped() { return dropped; }
    public int getQueueSize() { return queueSize; }
    public long getCommandsSeen() { return commandsSeen; }
    public long getParseErrors() { return parseErrors; }
    public double getTotalReadPerSec() { return totalReadPerSec; }
    public double getTotalWritePerSec() { return totalWritePerSec; }
    public Map<String, Long> getDangerousCommands() { return dangerousCommands; }
    public List<Entry> getPatterns() { return patterns; }
    public List<Entry> getRawKeys() { return rawKeys; }
}
