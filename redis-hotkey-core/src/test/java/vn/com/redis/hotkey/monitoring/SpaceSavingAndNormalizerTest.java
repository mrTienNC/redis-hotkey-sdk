package vn.com.redis.hotkey.monitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

class SpaceSavingAndNormalizerTest {

    @Test
    void spaceSavingFindsHeavyHittersWithBoundedMemory() {
        SpaceSaving ss = new SpaceSaving(50);
        Random rnd = new Random(42);
        List<String> stream = new ArrayList<String>();
        // 3 hot keys account for ~60% of traffic, plus 50,000 cold keys that appear once each
        for (int i = 0; i < 6000; i++) stream.add("hot1");
        for (int i = 0; i < 3000; i++) stream.add("hot2");
        for (int i = 0; i < 1500; i++) stream.add("hot3");
        for (int i = 0; i < 50000; i++) stream.add("cold:" + i);
        Collections.shuffle(stream, rnd);
        for (String k : stream) {
            ss.offer(k, 1);
        }
        assertTrue(ss.size() <= 50, "memory must be bounded");
        List<SpaceSaving.Item> top = ss.top(3);
        assertEquals("hot1", top.get(0).getKey());
        assertEquals("hot2", top.get(1).getKey());
        assertEquals("hot3", top.get(2).getKey());
        // count is never lower than the actual value; the overestimate is <= error
        assertTrue(top.get(0).getCount() >= 6000);
        assertTrue(top.get(0).getCount() - top.get(0).getError() <= 6000);
    }

    @Test
    void spaceSavingWeightedIncrement() {
        SpaceSaving ss = new SpaceSaving(10);
        ss.offer("a", 10);
        ss.offer("b", 3);
        ss.offer("a", 10);
        assertEquals("a", ss.top(1).get(0).getKey());
        assertEquals(20, ss.top(1).get(0).getCount());
    }

    @Test
    void normalizerGroupsIdsUuidsAndHashes() {
        KeyNormalizer n = new KeyNormalizer(null, 128);
        assertEquals("user:{id}", n.normalize("user:123"));
        assertEquals("user:{id}:profile", n.normalize("user:99999:profile"));
        assertEquals("order_{id}", n.normalize("order_42"));
        assertEquals("session:{uuid}", n.normalize("session:3f2504e0-4f89-11d3-9a0c-0305e82c3301"));
        assertEquals("obj:{hash}", n.normalize("obj:507f1f77bcf86cd799439011"));
    }

    @Test
    void normalizerKeepsWordsWithDigitsInsideTokens() {
        KeyNormalizer n = new KeyNormalizer(null, 128);
        assertEquals("cache:v2:config", n.normalize("cache:v2:config"));
        assertEquals("app:oauth2:token", n.normalize("app:oauth2:token"));
    }

    @Test
    void normalizerCustomRuleRunsFirst() {
        List<String[]> rules = new ArrayList<String[]>();
        rules.add(new String[]{"campaign:[A-Z]{3}[0-9]+", "campaign:{code}"});
        KeyNormalizer n = new KeyNormalizer(rules, 128);
        assertEquals("campaign:{code}", n.normalize("campaign:ABC123"));
    }

    @Test
    void normalizerTruncatesLongKeys() {
        KeyNormalizer n = new KeyNormalizer(null, 10);
        assertEquals(10, n.normalize("abcdefghijklmnopqrstuvwxyz").length());
    }
}
