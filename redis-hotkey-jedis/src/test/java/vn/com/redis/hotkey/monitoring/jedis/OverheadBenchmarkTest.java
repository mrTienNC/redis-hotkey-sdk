package vn.com.redis.hotkey.monitoring.jedis;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.HotKeyMonitor;
import vn.com.redis.hotkey.monitoring.resp.RespCommandSniffer;

/**
 * Measures overhead. NOT run by default; enable with -Dhotkey.bench=true (needs Redis at -Dhotkey.test.redis).
 * This is a practical measurement (not JMH): use it for relative comparison, not for absolute numbers.
 */
class OverheadBenchmarkTest {

    private static byte[] getCmd(String key) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        String head = "*2\r\n$3\r\nGET\r\n$" + k.length + "\r\n";
        byte[] h = head.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[h.length + k.length + 2];
        System.arraycopy(h, 0, out, 0, h.length);
        System.arraycopy(k, 0, out, h.length, k.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    @Test
    void snifferCpuCostPerCommand() throws Exception {
        assumeTrue(Boolean.getBoolean("hotkey.bench"));
        byte[] cmd = getCmd("user:1234567");
        System.out.println("=== A. Sniffer CPU cost per GET command (1 thread, after warm-up) ===");
        for (double rate : new double[]{0.0, 0.01, 0.1, 1.0}) {
            HotKeyConfig c = new HotKeyConfig();
            c.setSampleRate(rate == 0.0 ? 0.0001 : rate);
            c.setPublishIntervalSeconds(3600);
            HotKeyMonitor m = HotKeyMonitor.start(c);
            try {
                RespCommandSniffer s = new RespCommandSniffer(GlobalCommandSink.INSTANCE, 128, 64);
                for (int i = 0; i < 2000000; i++) {
                    s.feed(cmd, 0, cmd.length);
                }
                int n = 5000000;
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++) {
                    s.feed(cmd, 0, cmd.length);
                }
                long ns = System.nanoTime() - t0;
                System.out.printf("sampleRate=%-6s : %6.0f ns/command  (dropped=%d)%n",
                        rate == 0.0 ? "~0" : String.valueOf(rate), ns / (double) n, m.getTracker().getDropped());
            } finally {
                m.close();
            }
        }
    }

    @Test
    void endToEndLatency() throws Exception {
        assumeTrue(Boolean.getBoolean("hotkey.bench"));
        String[] hp = System.getProperty("hotkey.test.redis", "127.0.0.1:16379").split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        System.out.println("=== B. End-to-end GET against a real Redis (" + host + ":" + port + "), 4 threads x 4s, 1000 keys ===");
        System.out.printf("%-34s %10s %9s %9s %9s%n", "mode", "ops/s", "p50(us)", "p99(us)", "p99.9(us)");
        run("plain JedisPool (no SDK)", false, null, host, port);
        run("SDK, no monitor yet (no-op)", true, null, host, port);
        for (double r : new double[]{0.01, 0.1, 1.0}) {
            run("SDK sampleRate=" + r, true, Double.valueOf(r), host, port);
        }
        run("plain JedisPool (repeat)", false, null, host, port);
    }

    private void run(String label, boolean sdk, Double rate, String host, int port) throws Exception {
        HotKeyMonitor monitor = null;
        if (rate != null) {
            HotKeyConfig c = new HotKeyConfig();
            c.setSampleRate(rate);
            c.setPublishIntervalSeconds(3600);
            monitor = HotKeyMonitor.start(c);
        }
        JedisPoolConfig pc = new JedisPoolConfig();
        pc.setMaxTotal(16);
        final JedisPool pool = sdk ? HotKeyJedis.newPool(pc, host, port, 2000, null)
                : new JedisPool(pc, host, port, 2000);
        try (Jedis j = pool.getResource()) {
            for (int i = 0; i < 1000; i++) {
                j.set("bench:" + i, "value-" + i);
            }
        }
        final int threads = 4;
        final long durationNs = 4000000000L;
        final long[][] lat = new long[threads][];
        final int[] count = new int[threads];
        Thread[] ts = new Thread[threads];
        // warm-up 1.5s
        timedLoop(pool, 1500000000L, new long[400000], new int[1], 1);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            lat[t] = new long[600000];
            ts[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    count[id] = timedLoop(pool, durationNs, lat[id], new int[1], id + 7);
                }
            });
        }
        long t0 = System.nanoTime();
        for (Thread t : ts) t.start();
        for (Thread t : ts) t.join();
        double secs = (System.nanoTime() - t0) / 1e9;
        int total = 0;
        for (int c : count) total += c;
        long[] all = new long[total];
        int p = 0;
        for (int t = 0; t < threads; t++) {
            System.arraycopy(lat[t], 0, all, p, count[t]);
            p += count[t];
        }
        Arrays.sort(all);
        System.out.printf("%-34s %10.0f %9.0f %9.0f %9.0f%n", label, total / secs,
                all[(int) (total * 0.50)] / 1000.0, all[(int) (total * 0.99)] / 1000.0,
                all[(int) (total * 0.999)] / 1000.0);
        pool.close();
        if (monitor != null) {
            monitor.close();
        }
    }

    private static int timedLoop(JedisPool pool, long durationNs, long[] out, int[] unused, int seed) {
        Random rnd = new Random(seed);
        int n = 0;
        long end = System.nanoTime() + durationNs;
        while (System.nanoTime() < end && n < out.length) {
            String key = "bench:" + rnd.nextInt(1000);
            long t0 = System.nanoTime();
            try (Jedis j = pool.getResource()) {
                j.get(key);
            }
            out[n++] = System.nanoTime() - t0;
        }
        return n;
    }
}
