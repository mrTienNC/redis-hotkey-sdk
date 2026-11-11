package vn.com.redis.hotkey.monitoring;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Normalizes keys into patterns for grouping: {@code user:123} and {@code user:456} both become {@code user:{id}}.
 * Rules from the configuration are applied first, then the default rules (uuid, long hash, number).
 * Not thread-safe (has an internal LRU cache): use only from the aggregator thread.
 */
public final class KeyNormalizer {

    private static final int CACHE_SIZE = 10000;

    private static final class Rule {
        final Pattern pattern;
        final String replacement;

        Rule(String regex, String replacement) {
            this.pattern = Pattern.compile(regex);
            this.replacement = replacement;
        }
    }

    private final List<Rule> rules = new ArrayList<Rule>();
    private final int maxLength;
    private final Map<String, String> cache = new LinkedHashMap<String, String>(1024, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    public KeyNormalizer(List<String[]> customRules, int maxLength) {
        this.maxLength = maxLength;
        if (customRules != null) {
            for (String[] r : customRules) {
                rules.add(new Rule(r[0], r[1]));
            }
        }
        rules.add(new Rule("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "{uuid}"));
        rules.add(new Rule("(?<![0-9A-Za-z])(?=[0-9a-fA-F]*[0-9])(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{16,}(?![0-9A-Za-z])", "{hash}"));
        rules.add(new Rule("(?<![A-Za-z])[0-9]+(?![A-Za-z])", "{id}"));
    }

    public String normalize(String key) {
        String cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        String s = key.length() > maxLength ? key.substring(0, maxLength) : key;
        for (Rule r : rules) {
            s = r.pattern.matcher(s).replaceAll(r.replacement);
        }
        cache.put(key, s);
        return s;
    }
}
