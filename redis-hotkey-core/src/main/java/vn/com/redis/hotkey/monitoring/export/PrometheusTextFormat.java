package vn.com.redis.hotkey.monitoring.export;

import java.util.Locale;
import java.util.Map;

import vn.com.redis.hotkey.monitoring.Snapshot;

/**
 * Prometheus text exposition format (version 0.0.4), hand-written to avoid a library dependency.
 * Only exports the Top-N PATTERNs (never raw keys) to avoid cardinality explosion; a series disappears on its own when its
 * pattern drops out of the Top-N, because every render uses the latest snapshot.
 */
public final class PrometheusTextFormat {

    private PrometheusTextFormat() {
    }

    public static String render(Snapshot s) {
        StringBuilder sb = new StringBuilder(1024);
        String app = esc(s.getAppName());

        sb.append("# HELP redis_hotkey_ops_per_second Estimated operations/second per key pattern in the sliding window.\n");
        sb.append("# TYPE redis_hotkey_ops_per_second gauge\n");
        for (Snapshot.Entry e : s.getPatterns()) {
            line(sb, "redis_hotkey_ops_per_second", app, e.getName(), "read", e.getReadPerSec());
            line(sb, "redis_hotkey_ops_per_second", app, e.getName(), "write", e.getWritePerSec());
        }

        sb.append("# HELP redis_hotkey_total_ops_per_second Total operations/second (all keys) in the sliding window.\n");
        sb.append("# TYPE redis_hotkey_total_ops_per_second gauge\n");
        sb.append("redis_hotkey_total_ops_per_second{app=\"").append(app).append("\",type=\"read\"} ")
                .append(num(s.getTotalReadPerSec())).append('\n');
        sb.append("redis_hotkey_total_ops_per_second{app=\"").append(app).append("\",type=\"write\"} ")
                .append(num(s.getTotalWritePerSec())).append('\n');

        gauge(sb, "redis_hotkey_tracker_enabled", "1 if the SDK is enabled.", app, s.isEnabled() ? 1 : 0);
        gauge(sb, "redis_hotkey_tracker_sample_rate", "Current sampling rate.", app, s.getSampleRate());
        gauge(sb, "redis_hotkey_tracker_queue_size", "Number of samples waiting to be processed.", app, s.getQueueSize());
        counter(sb, "redis_hotkey_tracker_dropped_total", "Number of samples dropped because the queue was full.", app, s.getDropped());
        counter(sb, "redis_hotkey_commands_seen_total", "Total Redis commands observed by the SDK (before sampling).", app, s.getCommandsSeen());
        counter(sb, "redis_hotkey_parse_errors_total", "Number of sniffer connections disabled due to format errors.", app, s.getParseErrors());

        sb.append("# HELP redis_hotkey_dangerous_commands_total Dangerous commands (KEYS, FLUSHALL...) seen.\n");
        sb.append("# TYPE redis_hotkey_dangerous_commands_total counter\n");
        for (Map.Entry<String, Long> e : s.getDangerousCommands().entrySet()) {
            sb.append("redis_hotkey_dangerous_commands_total{app=\"").append(app).append("\",command=\"")
                    .append(esc(e.getKey())).append("\"} ").append(e.getValue()).append('\n');
        }
        return sb.toString();
    }

    private static void line(StringBuilder sb, String metric, String app, String pattern, String type, double v) {
        sb.append(metric).append("{app=\"").append(app).append("\",pattern=\"").append(esc(pattern))
                .append("\",type=\"").append(type).append("\"} ").append(num(v)).append('\n');
    }

    private static void gauge(StringBuilder sb, String name, String help, String app, double v) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append("{app=\"").append(app).append("\"} ").append(num(v)).append('\n');
    }

    private static void counter(StringBuilder sb, String name, String help, String app, long v) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        sb.append(name).append("{app=\"").append(app).append("\"} ").append(v).append('\n');
    }

    private static String num(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "0";
        }
        return String.format(Locale.ROOT, "%.3f", v);
    }

    /** Escapes a label value per the spec: backslash, double quote and newline. */
    static String esc(String v) {
        if (v == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(v.length() + 8);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == '\\') {
                sb.append("\\\\");
            } else if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\n') {
                sb.append("\\n");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
