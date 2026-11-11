package vn.com.redis.hotkey.monitoring.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.HotKeyMonitor;

/**
 * Enabled with {@code redis.hotkey.enabled=true}. Starts the {@link HotKeyMonitor} and activates exporters by name
 * from {@code redis.hotkey.exporters}. "Sniffing" Redis commands is the job of an adapter (e.g. redis-hotkey-jedis).
 */
@Configuration
@EnableConfigurationProperties(HotKeyProperties.class)
@ConditionalOnProperty(prefix = "redis.hotkey", name = "enabled", havingValue = "true")
public class HotKeyAutoConfiguration {

    @Bean(destroyMethod = "close")
    public HotKeyMonitor hotKeyMonitor(HotKeyProperties p, Environment env) {
        HotKeyConfig c = new HotKeyConfig();
        c.setEnabled(true);
        c.setAppName(p.getAppName() != null ? p.getAppName() : env.getProperty("spring.application.name", "app"));
        if (p.getInstanceId() != null) {
            c.setInstanceId(p.getInstanceId());
        }
        c.setSampleRate(p.getSampleRate());
        c.setQueueCapacity(p.getQueueCapacity());
        c.setWindowSeconds(p.getWindowSeconds());
        c.setBucketSeconds(p.getBucketSeconds());
        c.setPublishIntervalSeconds(p.getPublishIntervalSeconds());
        c.setTopN(p.getTopN());
        c.setExportRawKeys(p.isExportRawKeys());
        c.setMaxKeyLength(p.getMaxKeyLength());
        c.setExporters(p.getExporters());
        for (HotKeyProperties.Rule r : p.getNormalizerRules()) {
            if (r.getRegex() != null && r.getReplacement() != null) {
                c.addNormalizerRule(r.getRegex(), r.getReplacement());
            }
        }
        c.setPrometheusPort(p.getPrometheus().getPort());
        c.setPrometheusBindAddress(p.getPrometheus().getBindAddress());
        c.setPrometheusPath(p.getPrometheus().getPath());
        c.setTdAgentMode(p.getTdAgent().getMode());
        c.setTdAgentFilePath(p.getTdAgent().getFilePath());
        c.setTdAgentFileMaxBytes(p.getTdAgent().getFileMaxBytes());
        c.setTdAgentHttpUrl(p.getTdAgent().getHttpUrl());
        c.setTdAgentHttpTimeoutMs(p.getTdAgent().getHttpTimeoutMs());
        return HotKeyMonitor.start(c);
    }
}
