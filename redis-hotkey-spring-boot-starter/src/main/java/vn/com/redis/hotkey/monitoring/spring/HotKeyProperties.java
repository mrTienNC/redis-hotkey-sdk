package vn.com.redis.hotkey.monitoring.spring;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for {@code redis.hotkey.*}. Uses only simple getters/setters so it binds on Spring Boot 2.0 -> 3.x.
 * Defaults to {@code enabled=false}: it must be enabled explicitly.
 */
@ConfigurationProperties(prefix = "redis.hotkey")
public class HotKeyProperties {

    private boolean enabled = false;
    private String appName;
    private String instanceId;
    private double sampleRate = 0.1d;
    private int queueCapacity = 8192;
    private int windowSeconds = 60;
    private int bucketSeconds = 10;
    private int publishIntervalSeconds = 10;
    private int topN = 20;
    private boolean exportRawKeys = false;
    private int maxKeyLength = 128;
    /** Names of the exporters to activate: prometheus, td-agent. */
    private List<String> exporters = new ArrayList<String>();
    private List<Rule> normalizerRules = new ArrayList<Rule>();
    private final Prometheus prometheus = new Prometheus();
    private final TdAgent tdAgent = new TdAgent();

    public static class Rule {
        private String regex;
        private String replacement;

        public String getRegex() { return regex; }
        public void setRegex(String regex) { this.regex = regex; }
        public String getReplacement() { return replacement; }
        public void setReplacement(String replacement) { this.replacement = replacement; }
    }

    public static class Prometheus {
        private int port = 9404;
        private String bindAddress = "0.0.0.0";
        private String path = "/metrics";

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getBindAddress() { return bindAddress; }
        public void setBindAddress(String bindAddress) { this.bindAddress = bindAddress; }
        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
    }

    public static class TdAgent {
        /** file | stdout | http */
        private String mode = "file";
        private String filePath = "logs/redis-hotkey.log";
        private long fileMaxBytes = 50L * 1024 * 1024;
        private String httpUrl = "http://127.0.0.1:9880/redis.hotkey";
        private int httpTimeoutMs = 500;

        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public String getFilePath() { return filePath; }
        public void setFilePath(String filePath) { this.filePath = filePath; }
        public long getFileMaxBytes() { return fileMaxBytes; }
        public void setFileMaxBytes(long fileMaxBytes) { this.fileMaxBytes = fileMaxBytes; }
        public String getHttpUrl() { return httpUrl; }
        public void setHttpUrl(String httpUrl) { this.httpUrl = httpUrl; }
        public int getHttpTimeoutMs() { return httpTimeoutMs; }
        public void setHttpTimeoutMs(int httpTimeoutMs) { this.httpTimeoutMs = httpTimeoutMs; }
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
    public boolean isExportRawKeys() { return exportRawKeys; }
    public void setExportRawKeys(boolean exportRawKeys) { this.exportRawKeys = exportRawKeys; }
    public int getMaxKeyLength() { return maxKeyLength; }
    public void setMaxKeyLength(int maxKeyLength) { this.maxKeyLength = maxKeyLength; }
    public List<String> getExporters() { return exporters; }
    public void setExporters(List<String> exporters) { this.exporters = exporters; }
    public List<Rule> getNormalizerRules() { return normalizerRules; }
    public void setNormalizerRules(List<Rule> normalizerRules) { this.normalizerRules = normalizerRules; }
    public Prometheus getPrometheus() { return prometheus; }
    public TdAgent getTdAgent() { return tdAgent; }
}
