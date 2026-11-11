package vn.com.redis.hotkey.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.com.redis.hotkey.monitoring.export.ExporterFactory;
import vn.com.redis.hotkey.monitoring.export.PrometheusExporter;
import vn.com.redis.hotkey.monitoring.export.PrometheusTextFormat;
import vn.com.redis.hotkey.monitoring.export.SnapshotJson;

class MonitorPipelineTest {

    private HotKeyMonitor monitor;

    @AfterEach
    void tearDown() {
        if (monitor != null) {
            monitor.close();
        }
    }

    private HotKeyConfig baseConfig() {
        HotKeyConfig c = new HotKeyConfig();
        c.setAppName("test-app");
        c.setSampleRate(1.0d);
        c.setPublishIntervalSeconds(3600); // the test calls snapshotNow() manually
        return c;
    }

    private void waitDrained(HotKeyTracker t) throws InterruptedException {
        for (int i = 0; i < 100 && t.getQueueSize() > 0; i++) {
            Thread.sleep(20);
        }
        Thread.sleep(100); // let the aggregator process the last samples
    }

    @Test
    void hotPatternRanksFirstAndReadWriteAreSplit() throws Exception {
        monitor = HotKeyMonitor.start(baseConfig());
        HotKeyTracker t = monitor.getTracker();
        for (int i = 0; i < 900; i++) {
            t.record("product:detail:" + (i % 50), false);
        }
        for (int i = 0; i < 100; i++) {
            t.record("cart:" + i, true);
        }
        waitDrained(t);
        Snapshot s = monitor.snapshotNow();
        assertEquals("product:detail:{id}", s.getPatterns().get(0).getName());
        assertEquals("cart:{id}", s.getPatterns().get(1).getName());
        assertTrue(s.getPatterns().get(0).getReadPerSec() > 0);
        assertEquals(0.0d, s.getPatterns().get(0).getWritePerSec(), 0.0001);
        assertTrue(s.getPatterns().get(1).getWritePerSec() > 0);
        assertTrue(s.getRawKeys().isEmpty(), "raw keys are not exported by default");
    }

    @Test
    void sampleRateIsCompensatedByWeight() throws Exception {
        HotKeyConfig c = baseConfig();
        c.setSampleRate(0.5d);
        monitor = HotKeyMonitor.start(c);
        HotKeyTracker t = monitor.getTracker();
        // Calling record() directly does not sample; in real use the sniffer already filters via shouldSample().
        // Each sample carries weight = 2, so 100 samples -> an estimated 200 operations
        for (int i = 0; i < 100; i++) {
            t.record("k:" + i, false);
        }
        waitDrained(t);
        Snapshot s = monitor.snapshotNow();
        double estimatedOps = s.getTotalReadPerSec() * s.getWindowSeconds();
        // seconds is clamped to a minimum of 1s and a maximum of the window: rate * seconds is unstable right after startup,
        // so only check the relative ratio: double the result obtained with weight = 1
        HotKeyConfig c2 = baseConfig();
        monitor.close();
        monitor = HotKeyMonitor.start(c2);
        for (int i = 0; i < 100; i++) {
            monitor.getTracker().record("k:" + i, false);
        }
        waitDrained(monitor.getTracker());
        Snapshot s2 = monitor.snapshotNow();
        assertEquals(2.0d, s.getTotalReadPerSec() / s2.getTotalReadPerSec(), 0.2d);
        assertTrue(estimatedOps > 0);
    }

    @Test
    void disabledTrackerRecordsNothing() throws Exception {
        HotKeyConfig c = baseConfig();
        c.setEnabled(false);
        monitor = HotKeyMonitor.start(c);
        HotKeyTracker t = monitor.getTracker();
        assertFalse(t.shouldSample());
        t.record("a", false);
        t.onCommand("GET");
        waitDrained(t);
        Snapshot s = monitor.snapshotNow();
        assertTrue(s.getPatterns().isEmpty());
        assertEquals(0, s.getCommandsSeen());
        // re-enable at runtime
        t.setEnabled(true);
        t.record("a", false);
        waitDrained(t);
        assertEquals(1, monitor.snapshotNow().getPatterns().size());
    }

    @Test
    void fullQueueDropsInsteadOfBlocking() {
        HotKeyTracker t = new HotKeyTracker(16, 1.0d, true); // no aggregator draining samples
        long start = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            t.record("k" + i, false);
        }
        long ms = (System.nanoTime() - start) / 1000000L;
        assertTrue(t.getDropped() > 99000, "dropped=" + t.getDropped());
        assertTrue(ms < 2000, "record() must not block, took " + ms + "ms");
    }

    @Test
    void dangerousCommandsAreCounted() throws Exception {
        monitor = HotKeyMonitor.start(baseConfig());
        monitor.getTracker().onCommand("KEYS");
        monitor.getTracker().onCommand("KEYS");
        monitor.getTracker().onCommand("GET");
        Snapshot s = monitor.snapshotNow();
        assertEquals(Long.valueOf(2), s.getDangerousCommands().get("KEYS"));
        assertEquals(3, s.getCommandsSeen());
    }

    @Test
    void rawKeysOnlyWhenExplicitlyEnabled() throws Exception {
        HotKeyConfig c = baseConfig();
        c.setExportRawKeys(true);
        monitor = HotKeyMonitor.start(c);
        for (int i = 0; i < 500; i++) {
            monitor.getTracker().record("user:42", false);
        }
        monitor.getTracker().record("user:7", false);
        waitDrained(monitor.getTracker());
        Snapshot s = monitor.snapshotNow();
        assertEquals("user:42", s.getRawKeys().get(0).getName());
        assertTrue(SnapshotJson.toJson(s).contains("\"top_keys\""));
    }

    @Test
    void prometheusFormatEscapesAndHasNoRawKeys() throws Exception {
        HotKeyConfig c = baseConfig();
        c.setExportRawKeys(true);
        monitor = HotKeyMonitor.start(c);
        monitor.getTracker().record("user:42", false);
        monitor.getTracker().record("we\"ird\\key\nx", true);
        waitDrained(monitor.getTracker());
        String text = PrometheusTextFormat.render(monitor.snapshotNow());
        assertTrue(text.contains("# TYPE redis_hotkey_ops_per_second gauge"));
        assertTrue(text.contains("pattern=\"user:{id}\""));
        assertFalse(text.contains("user:42"), "raw keys must never be exported to Prometheus");
        assertTrue(text.contains("we\\\"ird\\\\key\\nx"), "label must be escaped: " + text);
        assertTrue(text.contains("redis_hotkey_tracker_dropped_total{app=\"test-app\"} 0"));
    }

    @Test
    void prometheusExporterServesMetricsOverHttp() throws Exception {
        HotKeyConfig c = baseConfig();
        c.setPrometheusPort(0);
        c.setPrometheusBindAddress("127.0.0.1");
        PrometheusExporter e = (PrometheusExporter) ExporterFactory.create("Prometheus", c);
        e.start();
        try {
            monitor = HotKeyMonitor.start(baseConfig());
            monitor.getTracker().record("hot:1", false);
            waitDrained(monitor.getTracker());
            e.export(monitor.snapshotNow());
            HttpURLConnection h = (HttpURLConnection) new URL("http://127.0.0.1:" + e.getPort() + "/metrics").openConnection();
            assertEquals(200, h.getResponseCode());
            assertTrue(h.getContentType().startsWith("text/plain"));
            InputStream in = h.getInputStream();
            String body = new Scanner(in, "UTF-8").useDelimiter("\\A").next();
            assertTrue(body.contains("pattern=\"hot:{id}\""), body);
        } finally {
            e.close();
        }
    }

    @Test
    void tdAgentFileModeWritesJsonLinesAndRotates(@TempDir File dir) throws Exception {
        HotKeyConfig c = baseConfig();
        File f = new File(dir, "sub/hotkey.log");
        c.setTdAgentMode("file");
        c.setTdAgentFilePath(f.getPath());
        c.setTdAgentFileMaxBytes(1500);
        c.setExporters(java.util.Collections.singletonList("td-agent"));
        c.setPublishIntervalSeconds(1);
        monitor = HotKeyMonitor.start(c);
        monitor.getTracker().record("product:1", false);
        for (int i = 0; i < 60 && (!f.exists() || f.length() == 0); i++) {
            Thread.sleep(100);
        }
        assertTrue(f.exists() && f.length() > 0, "td-agent must write the file");
        List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
        String first = lines.get(0);
        assertTrue(first.startsWith("{") && first.endsWith("}"), first);
        assertTrue(first.contains("\"app\":\"test-app\""));
        assertTrue(first.contains("\"top\":["));
        // wait longer to exceed 1500 bytes -> rotation
        for (int i = 0; i < 120 && !new File(f.getPath() + ".1").exists(); i++) {
            Thread.sleep(100);
        }
        assertTrue(new File(f.getPath() + ".1").exists(), "file must rotate when it exceeds the size limit");
    }

    @Test
    void unknownExporterIsRejectedByFactoryButDoesNotBreakMonitor() {
        assertThrows(IllegalArgumentException.class, () -> ExporterFactory.create("no-such-exporter", baseConfig()));
        HotKeyConfig c = baseConfig();
        c.setExporters(java.util.Arrays.asList("no-such-exporter", "prometheus"));
        c.setPrometheusPort(0);
        c.setPrometheusBindAddress("127.0.0.1");
        monitor = HotKeyMonitor.start(c);
        assertNotNull(monitor.getTracker(), "a wrong name is only logged, the application keeps running");
    }

    @Test
    void portConflictDoesNotBreakMonitor() throws Exception {
        java.net.ServerSocket occupied = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"));
        try {
            HotKeyConfig c = baseConfig();
            c.setExporters(java.util.Collections.singletonList("prometheus"));
            c.setPrometheusBindAddress("127.0.0.1");
            c.setPrometheusPort(occupied.getLocalPort());
            monitor = HotKeyMonitor.start(c);
            assertNotNull(monitor.getTracker());
        } finally {
            occupied.close();
        }
    }
}
