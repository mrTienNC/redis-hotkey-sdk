package vn.com.redis.hotkey.monitoring.jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.HotKeyMonitor;
import vn.com.redis.hotkey.monitoring.Snapshot;

/**
 * Runs against a real Redis. Defaults to 127.0.0.1:16379; override with -Dhotkey.test.redis=host:port.
 * If no Redis is available the tests are skipped (not failed).
 */
class JedisIntegrationTest {

    private String host;
    private int port;
    private JedisPool pool;
    private HotKeyMonitor monitor;

    @BeforeEach
    void setUp() {
        String[] hp = System.getProperty("hotkey.test.redis", "127.0.0.1:16379").split(":");
        host = hp[0];
        port = Integer.parseInt(hp[1]);
        boolean up;
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(host, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        } finally {
            try { s.close(); } catch (Exception ignore) { }
        }
        assumeTrue(up, "No Redis at " + host + ":" + port + " -> skipping integration test");

        HotKeyConfig c = new HotKeyConfig();
        c.setAppName("it");
        c.setSampleRate(1.0d);
        c.setPublishIntervalSeconds(3600);
        monitor = HotKeyMonitor.start(c);
        pool = HotKeyJedis.newPool(new JedisPoolConfig(), host, port, 2000, null);
        try (Jedis j = pool.getResource()) {
            j.flushDB();
        }
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
        if (monitor != null) {
            monitor.close();
        }
    }

    private Snapshot snap() throws InterruptedException {
        Thread.sleep(300); // let the aggregator drain all samples
        return monitor.snapshotNow();
    }

    private static double ops(Snapshot s, String pattern) {
        for (Snapshot.Entry e : s.getPatterns()) {
            if (e.getName().equals(pattern)) {
                return e.getTotalPerSec() * s.getWindowSeconds() >= 0 ? e.getTotalPerSec() : 0;
            }
        }
        return -1;
    }

    @Test
    void behaviorIsUnchangedAndCommandsAreCaptured() throws Exception {
        try (Jedis j = pool.getResource()) {
            assertEquals("OK", j.set("user:1", "alice"));
            assertEquals("alice", j.get("user:1"));
            assertNull(j.get("user:missing"));
            j.hset("profile:1", "name", "alice");
            Map<String, String> m = j.hgetAll("profile:1");
            assertEquals("alice", m.get("name"));
        }
        Snapshot s = snap();
        assertTrue(ops(s, "user:{id}") > 0, "missing user:{id}: " + names(s));
        assertTrue(ops(s, "profile:{id}") > 0, "missing profile:{id}: " + names(s));
        assertEquals(0, s.getParseErrors());
    }

    @Test
    void pipelineCommandsAreCaptured() throws Exception {
        try (Jedis j = pool.getResource()) {
            for (int i = 0; i < 20; i++) {
                j.hset("order:" + i, "f", "v" + i);
            }
            Pipeline p = j.pipelined();
            List<Response<Map<String, String>>> rs = new ArrayList<Response<Map<String, String>>>();
            for (int i = 0; i < 20; i++) {
                rs.add(p.hgetAll("order:" + i));
            }
            p.sync();
            assertEquals("v7", rs.get(7).get().get("f"));
        }
        Snapshot s = snap();
        double order = ops(s, "order:{id}");
        assertTrue(order > 0, names(s));
        // 20 hset + 20 hgetAll = 40 operations, window >= 1s => total = 40 / seconds; check the read/write split
        Snapshot.Entry e = find(s, "order:{id}");
        assertTrue(e.getReadPerSec() > 0, "pipelined HGETALL must count as a read");
        assertTrue(e.getWritePerSec() > 0, "HSET must count as a write");
        assertEquals(1.0d, e.getReadPerSec() / e.getWritePerSec(), 0.01d, "20 reads / 20 writes");
        assertEquals(0, s.getParseErrors());
    }

    @Test
    void multiExecAndLuaAndMultiKeyAreCaptured() throws Exception {
        try (Jedis j = pool.getResource()) {
            Transaction t = j.multi();
            t.set("tx:1", "a");
            t.set("tx:2", "b");
            t.exec();
            j.mset("m:1", "x", "m:2", "y");
            List<String> got = j.mget("m:1", "m:2");
            assertEquals("x", got.get(0));
            Object r = j.eval("return redis.call('get', KEYS[1])", 1, "tx:1");
            assertEquals("a", r);
            assertEquals(2L, (long) j.del("m:1", "m:2"));
        }
        Snapshot s = snap();
        assertTrue(ops(s, "tx:{id}") > 0, names(s));
        Snapshot.Entry m = find(s, "m:{id}");
        assertNotNull(m, names(s));
        assertTrue(m.getWritePerSec() > 0 && m.getReadPerSec() > 0, "MSET/DEL are writes, MGET is a read");
        assertEquals(0, s.getParseErrors());
    }

    @Test
    void dangerousKeysCommandIsCounted() throws Exception {
        try (Jedis j = pool.getResource()) {
            j.set("a:1", "1");
            assertEquals(1, j.keys("a:*").size());
        }
        Snapshot s = snap();
        assertEquals(Long.valueOf(1), s.getDangerousCommands().get("KEYS"));
        assertNull(find(s, "a:*"), "the KEYS argument is not a key");
    }

    @Test
    void poolWorksWithoutAnyMonitor() throws Exception {
        monitor.close();
        monitor = null;
        try (Jedis j = pool.getResource()) {
            j.set("plain:1", "v");
            assertEquals("v", j.get("plain:1"));
        }
    }

    @Test
    void largeValuesAndConcurrencyStayConsistent() throws Exception {
        final StringBuilder big = new StringBuilder();
        for (int i = 0; i < 200000; i++) {
            big.append('x');
        }
        final String bigValue = big.toString();
        final List<Throwable> errors = new ArrayList<Throwable>();
        List<Thread> threads = new ArrayList<Thread>();
        for (int t = 0; t < 8; t++) {
            final int id = t;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 200; i++) {
                            try (Jedis j = pool.getResource()) {
                                j.set("big:" + id, bigValue);
                                assertEquals(bigValue.length(), j.get("big:" + id).length());
                            }
                        }
                    } catch (Throwable e) {
                        synchronized (errors) { errors.add(e); }
                    }
                }
            });
            threads.add(th);
            th.start();
        }
        for (Thread th : threads) {
            th.join();
        }
        assertTrue(errors.isEmpty(), "errors: " + errors);
        Snapshot s = snap();
        assertEquals(0, s.getParseErrors(), "a large value split into chunks must not misalign the sniffer");
        Snapshot.Entry e = find(s, "big:{id}");
        assertNotNull(e);
    }

    private static Snapshot.Entry find(Snapshot s, String name) {
        for (Snapshot.Entry e : s.getPatterns()) {
            if (e.getName().equals(name)) {
                return e;
            }
        }
        return null;
    }

    private static String names(Snapshot s) {
        Map<String, Double> m = new HashMap<String, Double>();
        for (Snapshot.Entry e : s.getPatterns()) {
            m.put(e.getName(), e.getTotalPerSec());
        }
        return m.toString();
    }
}
