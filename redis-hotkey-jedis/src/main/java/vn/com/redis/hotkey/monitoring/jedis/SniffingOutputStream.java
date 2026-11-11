package vn.com.redis.hotkey.monitoring.jedis;

import java.io.IOException;
import java.io.OutputStream;

import vn.com.redis.hotkey.monitoring.resp.RespCommandSniffer;

/**
 * Wraps the socket's OutputStream: the real write happens first, then the sniffer "sees" the same bytes.
 * Sniffer errors never propagate; write behavior is preserved 100%.
 */
final class SniffingOutputStream extends OutputStream {

    private final OutputStream delegate;
    private final RespCommandSniffer sniffer;

    SniffingOutputStream(OutputStream delegate, RespCommandSniffer sniffer) {
        this.delegate = delegate;
        this.sniffer = sniffer;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        sniffer.feed(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        sniffer.feed(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
