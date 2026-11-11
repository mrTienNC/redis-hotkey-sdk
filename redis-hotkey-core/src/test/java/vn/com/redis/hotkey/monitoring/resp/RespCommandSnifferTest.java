package vn.com.redis.hotkey.monitoring.resp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class RespCommandSnifferTest {

    static final class Rec implements CommandSink {
        final List<String> commands = new ArrayList<String>();
        final List<String> keys = new ArrayList<String>();
        final List<Boolean> writes = new ArrayList<Boolean>();
        int parseErrors;
        boolean sample = true;

        @Override public void onCommand(String c) { commands.add(c); }
        @Override public boolean shouldSample() { return sample; }
        @Override public void onKey(String c, String k, boolean w) { keys.add(c + ":" + k); writes.add(w); }
        @Override public void onParseError() { parseErrors++; }
    }

    /** Builds a RESP command the same way Jedis sends it. */
    static byte[] cmd(String... args) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        w(o, "*" + args.length + "\r\n");
        for (String a : args) {
            byte[] b = a.getBytes(StandardCharsets.UTF_8);
            w(o, "$" + b.length + "\r\n");
            o.write(b, 0, b.length);
            w(o, "\r\n");
        }
        return o.toByteArray();
    }

    private static void w(ByteArrayOutputStream o, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        o.write(b, 0, b.length);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.write(p, 0, p.length);
        }
        return o.toByteArray();
    }

    @Test
    void getSingleKey() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("GET", "user:123");
        s.feed(b, 0, b.length);
        assertEquals(1, r.commands.size());
        assertEquals("GET", r.commands.get(0));
        assertEquals("GET:user:123", r.keys.get(0));
        assertFalse(r.writes.get(0), "GET is a read command");
    }

    @Test
    void setIsWriteAndValueIsNotCaptured() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("SET", "k1", "secret-value-should-never-appear");
        s.feed(b, 0, b.length);
        assertEquals(1, r.keys.size());
        assertEquals("SET:k1", r.keys.get(0));
        assertTrue(r.writes.get(0));
    }

    @Test
    void lowerCaseCommandIsNormalized() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("hgetall", "h1");
        s.feed(b, 0, b.length);
        assertEquals("HGETALL", r.commands.get(0));
        assertFalse(r.writes.get(0));
    }

    @Test
    void chunkedOneByteAtATime() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = concat(cmd("GET", "user:1"), cmd("HSET", "h", "f", "v"), cmd("GET", "user:2"));
        for (byte x : b) {
            s.feed(x);
        }
        assertEquals(3, r.commands.size());
        assertEquals("GET:user:1", r.keys.get(0));
        assertEquals("HSET:h", r.keys.get(1));
        assertEquals("GET:user:2", r.keys.get(2));
        assertEquals(0, r.parseErrors);
    }

    @Test
    void pipelineInOneBuffer() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = concat(cmd("HGETALL", "u:1"), cmd("HGETALL", "u:2"), cmd("EXPIRE", "u:3", "60"));
        s.feed(b, 0, b.length);
        assertEquals(3, r.keys.size());
        assertEquals("EXPIRE:u:3", r.keys.get(2));
    }

    @Test
    void multiKeyDelAndMget() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = concat(cmd("DEL", "a", "b", "c"), cmd("MGET", "x", "y"));
        s.feed(b, 0, b.length);
        assertEquals(5, r.keys.size());
        assertEquals("DEL:c", r.keys.get(2));
        assertEquals("MGET:y", r.keys.get(4));
    }

    @Test
    void msetTakesOnlyKeysNotValues() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("MSET", "k1", "v1", "k2", "v2");
        s.feed(b, 0, b.length);
        assertEquals(2, r.keys.size());
        assertEquals("MSET:k1", r.keys.get(0));
        assertEquals("MSET:k2", r.keys.get(1));
    }

    @Test
    void evalTakesDeclaredKeysOnly() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("EVAL", "return redis.call('get', KEYS[1])", "2", "ka", "kb", "argv1");
        s.feed(b, 0, b.length);
        assertEquals(2, r.keys.size());
        assertEquals("EVAL:ka", r.keys.get(0));
        assertEquals("EVAL:kb", r.keys.get(1));
    }

    @Test
    void commandsWithoutKeysAreNotRecordedAsKeys() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = concat(cmd("AUTH", "my-password"), cmd("SELECT", "3"), cmd("PING"), cmd("PUBLISH", "chan", "msg"),
                cmd("KEYS", "user:*"));
        s.feed(b, 0, b.length);
        assertEquals(5, r.commands.size());
        assertTrue(r.keys.isEmpty(), "AUTH/SELECT/PING/PUBLISH/KEYS must not be treated as keys: " + r.keys);
    }

    @Test
    void notSampledSkipsKeyExtraction() {
        Rec r = new Rec();
        r.sample = false;
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = concat(cmd("GET", "a"), cmd("GET", "b"));
        s.feed(b, 0, b.length);
        assertEquals(2, r.commands.size(), "commands are still counted");
        assertTrue(r.keys.isEmpty());
    }

    @Test
    void longKeyIsTruncated() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 16, 64);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append('x');
        }
        byte[] b = concat(cmd("GET", sb.toString()), cmd("GET", "next"));
        s.feed(b, 0, b.length);
        assertEquals("GET:xxxxxxxxxxxxxxxx", r.keys.get(0));
        assertEquals("GET:next", r.keys.get(1), "must not get misaligned after truncating a long key");
    }

    @Test
    void maxKeysPerCommandIsEnforced() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 3);
        byte[] b = concat(cmd("DEL", "a", "b", "c", "d", "e"), cmd("GET", "after"));
        s.feed(b, 0, b.length);
        assertEquals(4, r.keys.size());
        assertEquals("GET:after", r.keys.get(3));
    }

    @Test
    void largeValueIsSkippedAndStreamStaysAligned() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            big.append('v');
        }
        byte[] b = concat(cmd("SET", "k", big.toString()), cmd("GET", "k2"));
        // split into 1000-byte chunks
        for (int off = 0; off < b.length; off += 1000) {
            s.feed(b, off, Math.min(1000, b.length - off));
        }
        assertEquals(2, r.keys.size());
        assertEquals("GET:k2", r.keys.get(1));
        assertEquals(0, r.parseErrors);
    }

    @Test
    void garbageDisablesSnifferForThatConnection() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] bad = "not resp at all".getBytes(StandardCharsets.UTF_8);
        s.feed(bad, 0, bad.length);
        assertEquals(1, r.parseErrors);
        byte[] ok = cmd("GET", "a");
        s.feed(ok, 0, ok.length);
        assertTrue(r.keys.isEmpty(), "once broken, skip the stream instead of guessing");
        assertEquals(1, r.parseErrors, "the error is reported only once");
    }

    @Test
    void utf8KeyIsDecoded() {
        Rec r = new Rec();
        RespCommandSniffer s = new RespCommandSniffer(r, 128, 64);
        byte[] b = cmd("GET", "khóa:tiếng-việt");
        s.feed(b, 0, b.length);
        assertEquals("GET:khóa:tiếng-việt", r.keys.get(0));
    }
}
