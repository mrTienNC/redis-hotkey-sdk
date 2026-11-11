package vn.com.redis.hotkey.monitoring.jedis;

import vn.com.redis.hotkey.monitoring.HotKeyMonitor;
import vn.com.redis.hotkey.monitoring.HotKeyTracker;
import vn.com.redis.hotkey.monitoring.resp.CommandSink;

/**
 * Forwards events to the tracker of {@link HotKeyMonitor#global()}, looked up on every call.
 * This lets a pool be created before the monitor starts (or while the monitor is stopped/replaced) without recreating connections.
 * When there is no monitor, every method is a no-op.
 */
final class GlobalCommandSink implements CommandSink {

    static final GlobalCommandSink INSTANCE = new GlobalCommandSink();

    private GlobalCommandSink() {
    }

    private static HotKeyTracker tracker() {
        HotKeyMonitor m = HotKeyMonitor.global();
        return m == null ? null : m.getTracker();
    }

    @Override
    public void onCommand(String command) {
        HotKeyTracker t = tracker();
        if (t != null) {
            t.onCommand(command);
        }
    }

    @Override
    public boolean shouldSample() {
        HotKeyTracker t = tracker();
        return t != null && t.shouldSample();
    }

    @Override
    public void onKey(String command, String key, boolean write) {
        HotKeyTracker t = tracker();
        if (t != null) {
            t.onKey(command, key, write);
        }
    }

    @Override
    public void onParseError() {
        HotKeyTracker t = tracker();
        if (t != null) {
            t.onParseError();
        }
    }
}
