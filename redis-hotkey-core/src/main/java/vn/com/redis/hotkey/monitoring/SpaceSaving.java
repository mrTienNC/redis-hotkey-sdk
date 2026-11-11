package vn.com.redis.hotkey.monitoring;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Space-Saving algorithm (Metwally et al.) that finds the Top-K most frequent elements using fixed memory.
 * A count may exceed the true value by at most {@code error}, and is never lower.
 * Not thread-safe: use from a single thread (the aggregator).
 */
public final class SpaceSaving {

    /** Top-K result. */
    public static final class Item {
        private final String key;
        private final long count;
        private final long error;

        Item(String key, long count, long error) {
            this.key = key;
            this.count = count;
            this.error = error;
        }

        public String getKey() { return key; }
        public long getCount() { return count; }
        /** Maximum error of count (the true count lies within [count - error, count]). */
        public long getError() { return error; }
    }

    private static final class Node {
        final String key;
        long count;
        final long error;
        long seq;

        Node(String key, long count, long error, long seq) {
            this.key = key;
            this.count = count;
            this.error = error;
            this.seq = seq;
        }
    }

    private static final Comparator<Node> BY_COUNT_THEN_SEQ = new Comparator<Node>() {
        @Override
        public int compare(Node a, Node b) {
            if (a.count != b.count) {
                return a.count < b.count ? -1 : 1;
            }
            return a.seq < b.seq ? -1 : (a.seq == b.seq ? 0 : 1);
        }
    };

    private final int capacity;
    private final Map<String, Node> map;
    private final TreeSet<Node> order = new TreeSet<Node>(BY_COUNT_THEN_SEQ);
    private long seq;

    public SpaceSaving(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.map = new HashMap<String, Node>(Math.min(capacity * 2, 1 << 16));
    }

    public void offer(String key, long increment) {
        Node n = map.get(key);
        if (n != null) {
            order.remove(n); // must be removed before changing count so the TreeSet is not corrupted
            n.count += increment;
            n.seq = ++seq;
            order.add(n);
            return;
        }
        if (map.size() < capacity) {
            n = new Node(key, increment, 0L, ++seq);
        } else {
            Node min = order.pollFirst();
            map.remove(min.key);
            n = new Node(key, min.count + increment, min.count, ++seq);
        }
        map.put(key, n);
        order.add(n);
    }

    public int size() {
        return map.size();
    }

    /** Returns at most {@code limit} elements, in descending order of count. */
    public List<Item> top(int limit) {
        List<Item> out = new ArrayList<Item>(Math.min(limit, map.size()));
        for (Node n : order.descendingSet()) {
            if (out.size() >= limit) {
                break;
            }
            out.add(new Item(n.key, n.count, n.error));
        }
        return out;
    }

    public void clear() {
        map.clear();
        order.clear();
        seq = 0;
    }
}
