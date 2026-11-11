package vn.com.redis.hotkey.monitoring.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.HotKeyMonitor;

class HotKeyAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HotKeyAutoConfiguration.class));

    @Test
    void disabledByDefault() {
        runner.run(ctx -> {
            assertTrue(ctx.getBeansOfType(HotKeyMonitor.class).isEmpty());
            assertNull(HotKeyMonitor.global());
        });
    }

    @Test
    void enabledBindsPropertiesAndActivatesExportersByName() {
        runner.withPropertyValues(
                "redis.hotkey.enabled=true",
                "spring.application.name=order-service",
                "redis.hotkey.sample-rate=0.5",
                "redis.hotkey.window-seconds=30",
                "redis.hotkey.bucket-seconds=5",
                "redis.hotkey.exporters=prometheus,td-agent",
                "redis.hotkey.prometheus.port=0",
                "redis.hotkey.prometheus.bind-address=127.0.0.1",
                "redis.hotkey.td-agent.mode=stdout",
                "redis.hotkey.normalizer-rules[0].regex=campaign:[A-Z]+[0-9]+",
                "redis.hotkey.normalizer-rules[0].replacement=campaign:{code}")
                .run(ctx -> {
                    HotKeyMonitor m = ctx.getBean(HotKeyMonitor.class);
                    HotKeyConfig c = m.getConfig();
                    assertEquals("order-service", c.getAppName(), "taken from spring.application.name");
                    assertEquals(0.5d, c.getSampleRate(), 0.0001);
                    assertEquals(30, c.getWindowSeconds());
                    assertEquals(Arrays.asList("prometheus", "td-agent"), c.getExporters());
                    assertEquals("stdout", c.getTdAgentMode());
                    assertEquals(1, c.getNormalizerRules().size());
                    assertNotNull(HotKeyMonitor.global());
                    assertEquals(0.5d, m.getTracker().getSampleRate(), 0.0001);
                });
        assertNull(HotKeyMonitor.global(), "closing the context must shut down the monitor");
    }

    @Test
    void explicitFalseKeepsItOff() {
        runner.withPropertyValues("redis.hotkey.enabled=false").run(ctx ->
                assertTrue(ctx.getBeansOfType(HotKeyMonitor.class).isEmpty()));
    }
}
