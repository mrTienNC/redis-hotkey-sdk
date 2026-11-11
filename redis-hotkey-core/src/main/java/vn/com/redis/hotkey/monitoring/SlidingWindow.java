package vn.com.redis.hotkey.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sliding window implemented as a ring of buckets (e.g. 6 buckets x 10 seconds = 60 seconds).
 * Each bucket keeps per-pattern counts (with a bounded number of patterns) and an optional Top-K of raw keys (Space-Saving).
 * Not thread-safe: use only from the aggregator thread.
 */
final class SlidingWindow {

    static final String OTHER = "__other__";

    private static final class Bucket {
        long startMs = -1;
        final Map<String, long[]> patterns = new HashMap<String, long[]>();
        SpaceSaving rawKeys;
        long reads;
        long writes;

        void reset(long start, int rawCapacity) {
            startMs = start;
            patterns.clear();
            reads = 0;
            writes = 0;
            if (rawCapacity > 0) {
                if (rawKeys == null) {
                    rawKeys = new SpaceSaving(rawCapacity);
                } else {
                    rawKeys.clear();
                }
            }
        }
    }

    private final Bucket[] ring;
    private final long bucketMs;
    private final long windowMs;
    private final int patternCapacity;
    private final int rawCapacity;

    SlidingWindow(int windowSeconds, int bucketSeconds, int patternCapacity, int rawCapacity) {
        int b = Math.max(1, bucketSeconds);
        int w = Math.max(b, windowSeconds);
        this.bucketMs = b * 1000L;
        int n = (int) Math.ceil(w / (double) b);
        this.windowMs = n * bucketMs;
        this.ring = new Bucket[n];
        for (int i = 0; i < n; i++) {
            ring[i] = new Bucket();
        }
        this.patternCapacity = Math.max(1, patternCapacity);
        this.rawCapacity = rawCapacity;
    }

    long windowMs() {
        return windowMs;
    }

    void add(long nowMs, String pattern, String rawKey, boolean write, long weight) {
        long start = (nowMs / bucketMs) * bucketMs;
        Bucket bucket = ring[(int) ((nowMs / bucketMs) % ring.length)];
        if (bucket.startMs != start) {
            bucket.reset(start, rawCapacity);
        }
        long[] c = bucket.patterns.get(pattern);
        if (c == null) {
            if (bucket.patterns.size() >= patternCapacity) {
                pattern = OTHER;
                c = bucket.patterns.get(OTHER);
            }
            if (c == null) {
                c = new long[2];
                bucket.patterns.put(pattern, c);
            }
        }
        if (write) {
            c[1] += weight;
            bucket.writes += weight;
        } else {
            c[0] += weight;
            bucket.reads += weight;
        }
        if (rawKey != null && bucket.rawKeys != null) {
            bucket.rawKeys.offer(rawKey, weight);
        }
    }

    /** Merged result of the buckets still inside the window. */
    static final class Merged {
        final Map<String, long[]> patterns = new HashMap<String, long[]>();
        final Map<String, Long> rawKeys = new HashMap<String, Long>();
        long reads;
        long writes;
    }

    Merged merge(long nowMs) {
        Merged m = new Merged();
        long oldest = nowMs - windowMs;
        for (Bucket b : ring) {
            if (b.startMs < 0 || b.startMs + bucketMs <= oldest) {
                continue;
            }
            m.reads += b.reads;
            m.writes += b.writes;
            for (Map.Entry<String, long[]> e : b.patterns.entrySet()) {
                long[] t = m.patterns.get(e.getKey());
                if (t == null) {
                    t = new long[2];
                    m.patterns.put(e.getKey(), t);
                }
                t[0] += e.getValue()[0];
                t[1] += e.getValue()[1];
            }
            if (b.rawKeys != null) {
                for (SpaceSaving.Item it : b.rawKeys.top(Integer.MAX_VALUE)) {
                    Long prev = m.rawKeys.get(it.getKey());
                    m.rawKeys.put(it.getKey(), (prev == null ? 0L : prev) + it.getCount());
                }
            }
        }
        return m;
    }

    static List<Snapshot.Entry> topPatterns(Map<String, long[]> map, int n, double seconds) {
        List<Map.Entry<String, long[]>> list = new ArrayList<Map.Entry<String, long[]>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, long[]>>() {
            @Override
            public int compare(Map.Entry<String, long[]> a, Map.Entry<String, long[]> b) {
                long ta = a.getValue()[0] + a.getValue()[1];
                long tb = b.getValue()[0] + b.getValue()[1];
                return ta == tb ? 0 : (ta < tb ? 1 : -1);
            }
        });
        List<Snapshot.Entry> out = new ArrayList<Snapshot.Entry>();
        for (Map.Entry<String, long[]> e : list) {
            if (out.size() >= n) {
                break;
            }
            out.add(new Snapshot.Entry(e.getKey(), e.getValue()[0] / seconds, e.getValue()[1] / seconds));
        }
        return out;
    }

    static List<Snapshot.Entry> topRawKeys(Map<String, Long> map, int n, double seconds) {
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> a, Map.Entry<String, Long> b) {
                return b.getValue().compareTo(a.getValue());
            }
        });
        List<Snapshot.Entry> out = new ArrayList<Snapshot.Entry>();
        for (Map.Entry<String, Long> e : list) {
            if (out.size() >= n) {
                break;
            }
            // Space-Saving does not separate reads and writes: the total is shown as "read" for display; the total is still correct
            out.add(new Snapshot.Entry(e.getKey(), e.getValue() / seconds, 0.0d));
        }
        return out;
    }
}
