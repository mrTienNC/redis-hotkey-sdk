package vn.com.redis.hotkey.monitoring.export;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import vn.com.redis.hotkey.monitoring.Snapshot;

/** Converts a {@link Snapshot} into one JSON line (hand-written, no library dependency) for td-agent/Fluentd. */
public final class SnapshotJson {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private SnapshotJson() {
    }

    public static String toJson(Snapshot s) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        str(sb, "ts", ISO.format(Instant.ofEpochMilli(s.getTimestampMs()).atZone(ZoneId.systemDefault()))).append(',');
        str(sb, "app", s.getAppName()).append(',');
        str(sb, "instance", s.getInstanceId()).append(',');
        sb.append("\"window_sec\":").append(s.getWindowSeconds()).append(',');
        sb.append("\"sample_rate\":").append(num(s.getSampleRate())).append(',');
        sb.append("\"enabled\":").append(s.isEnabled()).append(',');
        sb.append("\"dropped\":").append(s.getDropped()).append(',');
        sb.append("\"queue_size\":").append(s.getQueueSize()).append(',');
        sb.append("\"commands_seen\":").append(s.getCommandsSeen()).append(',');
        sb.append("\"parse_errors\":").append(s.getParseErrors()).append(',');
        sb.append("\"total_read_per_sec\":").append(num(s.getTotalReadPerSec())).append(',');
        sb.append("\"total_write_per_sec\":").append(num(s.getTotalWritePerSec())).append(',');

        sb.append("\"dangerous\":{");
        boolean first = true;
        for (Map.Entry<String, Long> e : s.getDangerousCommands().entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(esc(e.getKey())).append("\":").append(e.getValue());
        }
        sb.append("},");

        sb.append("\"top\":[");
        List<Snapshot.Entry> p = s.getPatterns();
        for (int i = 0; i < p.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            Snapshot.Entry e = p.get(i);
            sb.append("{\"pattern\":\"").append(esc(e.getName())).append("\",\"ops_per_sec\":")
                    .append(num(e.getTotalPerSec())).append(",\"read\":").append(num(e.getReadPerSec()))
                    .append(",\"write\":").append(num(e.getWritePerSec())).append('}');
        }
        sb.append(']');

        // Raw keys are exported ONLY when export-raw-keys=true
        if (!s.getRawKeys().isEmpty()) {
            sb.append(",\"top_keys\":[");
            List<Snapshot.Entry> r = s.getRawKeys();
            for (int i = 0; i < r.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append("{\"key\":\"").append(esc(r.get(i).getName())).append("\",\"ops_per_sec\":")
                        .append(num(r.get(i).getTotalPerSec())).append('}');
            }
            sb.append(']');
        }
        sb.append('}');
        return sb.toString();
    }

    private static StringBuilder str(StringBuilder sb, String k, String v) {
        return sb.append('"').append(k).append("\":\"").append(esc(v)).append('"');
    }

    private static String num(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "0";
        }
        return String.format(Locale.ROOT, "%.3f", v);
    }

    static String esc(String v) {
        if (v == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(v.length() + 8);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
