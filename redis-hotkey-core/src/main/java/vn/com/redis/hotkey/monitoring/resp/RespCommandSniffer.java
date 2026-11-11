package vn.com.redis.hotkey.monitoring.resp;

import java.nio.charset.Charset;
import java.util.Locale;

import vn.com.redis.hotkey.monitoring.resp.CommandCatalog.KeyMode;

/**
 * Streaming RESP parser for the outgoing COMMAND stream (client -> server).
 * Every Redis client command is an array of bulk strings: {@code *N\r\n$len\r\nNAME\r\n$len\r\nKEY\r\n...}.
 * Data may arrive split at any position, so this is a state machine that processes chunk by chunk.
 *
 * <p>Only copies the bytes of the command name and of the keys to track (values are skipped), so the cost is proportional to the number of arguments,
 * not to the number of bytes. Passwords (AUTH) and value contents are never copied.
 *
 * <p>Not thread-safe: one instance per connection (Jedis uses a connection from one thread at a time).
 */
public final class RespCommandSniffer {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final Charset LATIN1 = Charset.forName("ISO-8859-1");

    private static final int MAX_ARGC = 1000000;
    private static final int MAX_COMMAND_NAME = 32;

    private static final int S_STAR = 0;
    private static final int S_ARGC = 1;
    private static final int S_ARGC_LF = 2;
    private static final int S_DOLLAR = 3;
    private static final int S_LEN = 4;
    private static final int S_LEN_LF = 5;
    private static final int S_PAYLOAD = 6;
    private static final int S_PAYLOAD_CR = 7;
    private static final int S_PAYLOAD_LF = 8;

    private final CommandSink sink;
    private final int maxKeyLength;
    private final int maxKeysPerCommand;
    private final byte[] cap;

    private int state = S_STAR;
    private boolean broken;

    private long num;
    private int argc;
    private int argIdx;
    private int bulkLen;
    private int bulkRead;
    private int capLen;
    private boolean capturing;

    private String command;
    private KeyMode mode = KeyMode.NONE;
    private boolean write;
    private boolean sampled;
    private int keysEmitted;
    private int evalNumKeys;

    public RespCommandSniffer(CommandSink sink, int maxKeyLength, int maxKeysPerCommand) {
        this.sink = sink;
        this.maxKeyLength = Math.max(8, maxKeyLength);
        this.maxKeysPerCommand = Math.max(1, maxKeysPerCommand);
        this.cap = new byte[Math.max(this.maxKeyLength, MAX_COMMAND_NAME)];
    }

    /** Dangerous Redis commands (blocking/global), e.g. KEYS, FLUSHALL. */
    public static boolean isDangerous(String upperCommand) {
        return CommandCatalog.isDangerous(upperCommand);
    }

    public void feed(int b) {
        feed(new byte[]{(byte) b}, 0, 1);
    }

    /** Feeds a chunk of bytes (just written to the socket) into the state machine. Never throws. */
    public void feed(byte[] buf, int off, int len) {
        if (broken) {
            return;
        }
        try {
            process(buf, off, off + len);
        } catch (Throwable t) {
            fail();
        }
    }

    private void fail() {
        broken = true;
        try {
            sink.onParseError();
        } catch (Throwable ignore) {
            // must not affect the application
        }
    }

    private void process(byte[] buf, int i, int end) {
        while (i < end) {
            switch (state) {
                case S_STAR:
                    if (buf[i++] != '*') {
                        fail();
                        return;
                    }
                    num = 0;
                    state = S_ARGC;
                    break;
                case S_ARGC:
                    i = readNumber(buf, i, end, S_ARGC_LF);
                    break;
                case S_ARGC_LF:
                    if (buf[i++] != '\n') {
                        fail();
                        return;
                    }
                    if (num <= 0 || num > MAX_ARGC) {
                        fail();
                        return;
                    }
                    argc = (int) num;
                    argIdx = 0;
                    command = null;
                    mode = KeyMode.NONE;
                    sampled = false;
                    keysEmitted = 0;
                    evalNumKeys = 0;
                    state = S_DOLLAR;
                    break;
                case S_DOLLAR:
                    if (buf[i++] != '$') {
                        fail();
                        return;
                    }
                    num = 0;
                    state = S_LEN;
                    break;
                case S_LEN:
                    i = readNumber(buf, i, end, S_LEN_LF);
                    break;
                case S_LEN_LF:
                    if (buf[i++] != '\n') {
                        fail();
                        return;
                    }
                    if (num < 0 || num > Integer.MAX_VALUE) {
                        fail();
                        return;
                    }
                    bulkLen = (int) num;
                    bulkRead = 0;
                    capLen = 0;
                    capturing = needCapture(argIdx);
                    state = bulkLen == 0 ? S_PAYLOAD_CR : S_PAYLOAD;
                    break;
                case S_PAYLOAD: {
                    int n = Math.min(end - i, bulkLen - bulkRead);
                    int limit = argIdx == 0 ? MAX_COMMAND_NAME : maxKeyLength;
                    if (capturing && capLen < limit) {
                        int c = Math.min(n, limit - capLen);
                        System.arraycopy(buf, i, cap, capLen, c);
                        capLen += c;
                    }
                    i += n;
                    bulkRead += n;
                    if (bulkRead == bulkLen) {
                        state = S_PAYLOAD_CR;
                    }
                    break;
                }
                case S_PAYLOAD_CR:
                    if (buf[i++] != '\r') {
                        fail();
                        return;
                    }
                    state = S_PAYLOAD_LF;
                    break;
                case S_PAYLOAD_LF:
                    if (buf[i++] != '\n') {
                        fail();
                        return;
                    }
                    onArgComplete();
                    argIdx++;
                    state = argIdx >= argc ? S_STAR : S_DOLLAR;
                    break;
                default:
                    fail();
                    return;
            }
        }
    }

    /** Reads digits until '\r'; then switches to state {@code lfState}. */
    private int readNumber(byte[] buf, int i, int end, int lfState) {
        while (i < end) {
            byte c = buf[i++];
            if (c == '\r') {
                state = lfState;
                return i;
            }
            if (c < '0' || c > '9') {
                fail();
                return end;
            }
            num = num * 10 + (c - '0');
            if (num > Integer.MAX_VALUE) {
                fail();
                return end;
            }
        }
        return i;
    }

    private boolean needCapture(int idx) {
        if (idx == 0) {
            return true;
        }
        if (!sampled) {
            return false;
        }
        switch (mode) {
            case FIRST:
                return idx == 1;
            case ALL_ARGS:
                return keysEmitted < maxKeysPerCommand;
            case EVEN_ARGS:
                return (idx & 1) == 1 && keysEmitted < maxKeysPerCommand;
            case EVAL:
                return idx == 2 || (idx >= 3 && idx < 3 + evalNumKeys && keysEmitted < maxKeysPerCommand);
            case ALL_BUT_LAST:
                return idx < argc - 1 && keysEmitted < maxKeysPerCommand;
            default:
                return false;
        }
    }

    private void onArgComplete() {
        if (argIdx == 0) {
            command = new String(cap, 0, Math.min(capLen, MAX_COMMAND_NAME), LATIN1).toUpperCase(Locale.ENGLISH);
            mode = CommandCatalog.modeOf(command);
            write = !CommandCatalog.isRead(command);
            sink.onCommand(command);
            sampled = mode != KeyMode.NONE && sink.shouldSample();
            return;
        }
        if (!capturing) {
            return;
        }
        if (mode == KeyMode.EVAL && argIdx == 2) {
            evalNumKeys = parseSmallInt(cap, capLen);
            return;
        }
        sink.onKey(command, new String(cap, 0, capLen, UTF8), write);
        keysEmitted++;
    }

    private static int parseSmallInt(byte[] b, int len) {
        int v = 0;
        for (int i = 0; i < len; i++) {
            byte c = b[i];
            if (c < '0' || c > '9') {
                return 0;
            }
            v = v * 10 + (c - '0');
            if (v > 10000) {
                return 10000;
            }
        }
        return v;
    }
}
