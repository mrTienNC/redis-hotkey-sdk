package vn.com.redis.hotkey.monitoring.export;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Locale;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.Snapshot;

/**
 * td-agent / Fluentd (push or tail). Writes one JSON line per cycle.
 * <ul>
 *   <li>{@code file}: writes JSON lines to a file for td-agent {@code in_tail} to read (rotates to 1 backup copy when the size limit is exceeded)</li>
 *   <li>{@code stdout}: prints to stdout, suitable for containers when the platform already collects stdout logs</li>
 *   <li>{@code http}: POSTs to td-agent {@code in_http} (short timeout, errors are ignored)</li>
 * </ul>
 */
public final class TdAgentExporter implements HotKeyExporter {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final HotKeyConfig config;
    private final String mode;
    private OutputStream fileOut;
    private File file;
    private long written;

    public TdAgentExporter(HotKeyConfig config) {
        this.config = config;
        this.mode = config.getTdAgentMode() == null ? "file" : config.getTdAgentMode().trim().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String name() {
        return "td-agent";
    }

    @Override
    public void start() throws IOException {
        if (!"file".equals(mode) && !"stdout".equals(mode) && !"http".equals(mode)) {
            throw new IllegalArgumentException("Invalid td-agent mode: " + mode + " (file|stdout|http)");
        }
        if ("file".equals(mode)) {
            file = new File(config.getTdAgentFilePath()).getAbsoluteFile();
            File dir = file.getParentFile();
            if (dir != null && !dir.exists() && !dir.mkdirs()) {
                throw new IOException("Could not create directory " + dir);
            }
            openFile();
        }
    }

    private void openFile() throws IOException {
        fileOut = new FileOutputStream(file, true);
        written = file.length();
    }

    @Override
    public synchronized void export(Snapshot snapshot) throws IOException {
        byte[] line = (SnapshotJson.toJson(snapshot) + "\n").getBytes(UTF8);
        if ("file".equals(mode)) {
            if (written + line.length > config.getTdAgentFileMaxBytes()) {
                rotate();
            }
            fileOut.write(line);
            fileOut.flush();
            written += line.length;
        } else if ("stdout".equals(mode)) {
            System.out.write(line, 0, line.length);
            System.out.flush();
        } else {
            post(line);
        }
    }

    private void rotate() throws IOException {
        fileOut.close();
        File old = new File(file.getPath() + ".1");
        if (old.exists() && !old.delete()) {
            throw new IOException("Could not delete " + old);
        }
        if (!file.renameTo(old)) {
            throw new IOException("Could not rename " + file);
        }
        openFile();
    }

    private void post(byte[] body) throws IOException {
        HttpURLConnection c = (HttpURLConnection) new URL(config.getTdAgentHttpUrl()).openConnection();
        try {
            int t = config.getTdAgentHttpTimeoutMs();
            c.setConnectTimeout(t);
            c.setReadTimeout(t);
            c.setRequestMethod("POST");
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            c.setFixedLengthStreamingMode(body.length);
            OutputStream os = c.getOutputStream();
            try {
                os.write(body);
            } finally {
                os.close();
            }
            int code = c.getResponseCode();
            if (code >= 300) {
                throw new IOException("td-agent in_http returned HTTP " + code);
            }
        } finally {
            c.disconnect();
        }
    }

    @Override
    public synchronized void close() {
        if (fileOut != null) {
            try {
                fileOut.close();
            } catch (IOException ignore) {
                // release resources on shutdown
            }
        }
    }
}
