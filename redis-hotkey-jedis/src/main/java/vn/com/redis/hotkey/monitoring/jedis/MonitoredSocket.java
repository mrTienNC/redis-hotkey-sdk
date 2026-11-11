package vn.com.redis.hotkey.monitoring.jedis;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

import vn.com.redis.hotkey.monitoring.HotKeyConfig;
import vn.com.redis.hotkey.monitoring.HotKeyMonitor;
import vn.com.redis.hotkey.monitoring.resp.RespCommandSniffer;

/**
 * Socket that delegates entirely to the real socket, except that its OutputStream is wrapped to sniff outgoing commands.
 * (Jedis only uses getOutputStream/getInputStream and the configuration/state methods, all of which are delegated.)
 */
final class MonitoredSocket extends Socket {

    private final Socket d;
    private OutputStream out;

    MonitoredSocket(Socket delegate) {
        this.d = delegate;
    }

    @Override
    public synchronized OutputStream getOutputStream() throws IOException {
        if (out == null) {
            int maxKeyLen = 128;
            int maxKeys = 64;
            HotKeyMonitor m = HotKeyMonitor.global();
            if (m != null) {
                HotKeyConfig c = m.getConfig();
                maxKeyLen = c.getMaxKeyLength();
                maxKeys = c.getMaxKeysPerCommand();
            }
            out = new SniffingOutputStream(d.getOutputStream(),
                    new RespCommandSniffer(GlobalCommandSink.INSTANCE, maxKeyLen, maxKeys));
        }
        return out;
    }

    @Override public InputStream getInputStream() throws IOException { return d.getInputStream(); }
    @Override public void connect(SocketAddress endpoint) throws IOException { d.connect(endpoint); }
    @Override public void connect(SocketAddress endpoint, int timeout) throws IOException { d.connect(endpoint, timeout); }
    @Override public void bind(SocketAddress bindpoint) throws IOException { d.bind(bindpoint); }
    @Override public InetAddress getInetAddress() { return d.getInetAddress(); }
    @Override public InetAddress getLocalAddress() { return d.getLocalAddress(); }
    @Override public int getPort() { return d.getPort(); }
    @Override public int getLocalPort() { return d.getLocalPort(); }
    @Override public SocketAddress getRemoteSocketAddress() { return d.getRemoteSocketAddress(); }
    @Override public SocketAddress getLocalSocketAddress() { return d.getLocalSocketAddress(); }
    @Override public SocketChannel getChannel() { return d.getChannel(); }
    @Override public void setTcpNoDelay(boolean on) throws SocketException { d.setTcpNoDelay(on); }
    @Override public boolean getTcpNoDelay() throws SocketException { return d.getTcpNoDelay(); }
    @Override public void setSoLinger(boolean on, int linger) throws SocketException { d.setSoLinger(on, linger); }
    @Override public int getSoLinger() throws SocketException { return d.getSoLinger(); }
    @Override public void sendUrgentData(int data) throws IOException { d.sendUrgentData(data); }
    @Override public void setOOBInline(boolean on) throws SocketException { d.setOOBInline(on); }
    @Override public boolean getOOBInline() throws SocketException { return d.getOOBInline(); }
    @Override public synchronized void setSoTimeout(int timeout) throws SocketException { d.setSoTimeout(timeout); }
    @Override public synchronized int getSoTimeout() throws SocketException { return d.getSoTimeout(); }
    @Override public synchronized void setSendBufferSize(int size) throws SocketException { d.setSendBufferSize(size); }
    @Override public synchronized int getSendBufferSize() throws SocketException { return d.getSendBufferSize(); }
    @Override public synchronized void setReceiveBufferSize(int size) throws SocketException { d.setReceiveBufferSize(size); }
    @Override public synchronized int getReceiveBufferSize() throws SocketException { return d.getReceiveBufferSize(); }
    @Override public void setKeepAlive(boolean on) throws SocketException { d.setKeepAlive(on); }
    @Override public boolean getKeepAlive() throws SocketException { return d.getKeepAlive(); }
    @Override public void setTrafficClass(int tc) throws SocketException { d.setTrafficClass(tc); }
    @Override public int getTrafficClass() throws SocketException { return d.getTrafficClass(); }
    @Override public void setReuseAddress(boolean on) throws SocketException { d.setReuseAddress(on); }
    @Override public boolean getReuseAddress() throws SocketException { return d.getReuseAddress(); }
    @Override public synchronized void close() throws IOException { d.close(); }
    @Override public void shutdownInput() throws IOException { d.shutdownInput(); }
    @Override public void shutdownOutput() throws IOException { d.shutdownOutput(); }
    @Override public String toString() { return d.toString(); }
    @Override public boolean isConnected() { return d.isConnected(); }
    @Override public boolean isBound() { return d.isBound(); }
    @Override public boolean isClosed() { return d.isClosed(); }
    @Override public boolean isInputShutdown() { return d.isInputShutdown(); }
    @Override public boolean isOutputShutdown() { return d.isOutputShutdown(); }
    @Override public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
        d.setPerformancePreferences(connectionTime, latency, bandwidth);
    }
}
