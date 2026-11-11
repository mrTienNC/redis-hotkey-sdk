package vn.com.redis.hotkey.monitoring.resp;

/**
 * Receiver of events from {@link RespCommandSniffer}. Every method runs on the application thread (hot path):
 * must be fast, non-blocking and must not throw.
 */
public interface CommandSink {

    /** Every command seen (name in UPPER CASE), including commands without a key. Used to count dangerous commands (KEYS, FLUSHALL...). */
    void onCommand(String command);

    /** Whether to sample this command. If it returns false the sniffer does not extract the key (saves CPU/memory). */
    boolean shouldSample();

    /** A key was accessed; {@code write} = write command. */
    void onKey(String command, String key, boolean write);

    /** Encountered a byte that is not valid RESP; the sniffer disables itself for that connection. */
    void onParseError();
}
