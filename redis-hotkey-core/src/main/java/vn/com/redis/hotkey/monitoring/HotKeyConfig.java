package vn.com.redis.hotkey.monitoring;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SDK configuration. Everything has a safe default; only {@link #setExporters} is needed to choose where data is exported.
 * Java 8, no external library dependencies.
 */
public class HotKeyConfig {

    /** Master switch. false = the SDK does nothing on the hot path. */
    private boolean enabled = true;
    private String appName = "app";
    private String instanceId = defaultInstanceId();

    /** Sampling rate for commands that carry a key, (0, 1]. Adjustable at runtime via {@link HotKeyTracker#setSampleRate}. */
    private double sampleRate = 0.1d;
    /** Size of the queue between the application thread and the aggregator. When full, samples are dropped (never blocks). */
    private int queueCapacity = 8192;

    private int windowSeconds = 60;
    private int bucketSeconds = 10;
    private int publishIntervalSeconds = 10;
    private int topN = 20;
    /** Maximum number of patterns tracked per bucket; the excess is merged into "__other__". */
    private int patternCapacity = 1000;

    /** Track exact keys (not normalized) using Space-Saving. OFF by default because keys may contain sensitive data. */
    private boolean exportRawKeys = false;
    private int rawKeyCapacity = 256;
    private int maxKeyLength = 128;
    private int maxKeysPerCommand = 64;

    /** Extra normalization rules: each element is {regex, replacement}, applied before the default rules. */
    private List<String[]> normalizerRules = new ArrayList<String[]>();

    /** Names of the exporters to enable, e.g. "prometheus", "td-agent". */
    private List<String> exporters = new ArrayList<String>();

    // prometheus
    private int prometheusPort = 9404;
    private String prometheusBindAddress = "0.0.0.0";
    private String prometheusPath = "/metrics";

    // td-agent / fluentd
    /** file | stdout | http */
    private String tdAgentMode = "file";
    private String tdAgentFilePath = "logs/redis-hotkey.log";
    private long tdAgentFileMaxBytes = 50L * 1024 * 1024;
    private String tdAgentHttpUrl = "http://127.0.0.1:9880/redis.hotkey";
    private int tdAgentHttpTimeoutMs = 500;

    public HotKeyConfig() {
    }

    private static String defaultInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Throwable t) {
            return "unknown";
        }
    }

    public HotKeyConfig addNormalizerRule(String regex, String replacement) {
        normalizerRules.add(new String[]{regex, replacement});
        return this;
    }

    public HotKeyConfig exporters(String... names) {
        this.exporters = new ArrayList<String>(Arrays.asList(names));
        return this;
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public double getSampleRate() { return sampleRate; }
    public void setSampleRate(double sampleRate) { this.sampleRate = sampleRate; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public int getWindowSeconds() { return windowSeconds; }
    public void setWindowSeconds(int windowSeconds) { this.windowSeconds = windowSeconds; }
    public int getBucketSeconds() { return bucketSeconds; }
    public void setBucketSeconds(int bucketSeconds) { this.bucketSeconds = bucketSeconds; }
    public int getPublishIntervalSeconds() { return publishIntervalSeconds; }
    public void setPublishIntervalSeconds(int publishIntervalSeconds) { this.publishIntervalSeconds = publishIntervalSeconds; }
    public int getTopN() { return topN; }
    public void setTopN(int topN) { this.topN = topN; }
    public int getPatternCapacity() { return patternCapacity; }
    public void setPatternCapacity(int patternCapacity) { this.patternCapacity = patternCapacity; }
    public boolean isExportRawKeys() { return exportRawKeys; }
    public void setExportRawKeys(boolean exportRawKeys) { this.exportRawKeys = exportRawKeys; }
    public int getRawKeyCapacity() { return rawKeyCapacity; }
    public void setRawKeyCapacity(int rawKeyCapacity) { this.rawKeyCapacity = rawKeyCapacity; }
    public int getMaxKeyLength() { return maxKeyLength; }
    public void setMaxKeyLength(int maxKeyLength) { this.maxKeyLength = maxKeyLength; }
    public int getMaxKeysPerCommand() { return maxKeysPerCommand; }
    public void setMaxKeysPerCommand(int maxKeysPerCommand) { this.maxKeysPerCommand = maxKeysPerCommand; }
    public List<String[]> getNormalizerRules() { return normalizerRules; }
    public void setNormalizerRules(List<String[]> normalizerRules) { this.normalizerRules = normalizerRules; }
    public List<String> getExporters() { return exporters; }
    public void setExporters(List<String> exporters) { this.exporters = exporters; }
    public int getPrometheusPort() { return prometheusPort; }
    public void setPrometheusPort(int prometheusPort) { this.prometheusPort = prometheusPort; }
    public String getPrometheusBindAddress() { return prometheusBindAddress; }
    public void setPrometheusBindAddress(String prometheusBindAddress) { this.prometheusBindAddress = prometheusBindAddress; }
    public String getPrometheusPath() { return prometheusPath; }
    public void setPrometheusPath(String prometheusPath) { this.prometheusPath = prometheusPath; }
    public String getTdAgentMode() { return tdAgentMode; }
    public void setTdAgentMode(String tdAgentMode) { this.tdAgentMode = tdAgentMode; }
    public String getTdAgentFilePath() { return tdAgentFilePath; }
    public void setTdAgentFilePath(String tdAgentFilePath) { this.tdAgentFilePath = tdAgentFilePath; }
    public long getTdAgentFileMaxBytes() { return tdAgentFileMaxBytes; }
    public void setTdAgentFileMaxBytes(long tdAgentFileMaxBytes) { this.tdAgentFileMaxBytes = tdAgentFileMaxBytes; }
    public String getTdAgentHttpUrl() { return tdAgentHttpUrl; }
    public void setTdAgentHttpUrl(String tdAgentHttpUrl) { this.tdAgentHttpUrl = tdAgentHttpUrl; }
    public int getTdAgentHttpTimeoutMs() { return tdAgentHttpTimeoutMs; }
    public void setTdAgentHttpTimeoutMs(int tdAgentHttpTimeoutMs) { this.tdAgentHttpTimeoutMs = tdAgentHttpTimeoutMs; }
}
