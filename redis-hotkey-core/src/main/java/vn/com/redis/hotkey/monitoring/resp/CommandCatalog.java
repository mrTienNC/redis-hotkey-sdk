package vn.com.redis.hotkey.monitoring.resp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Static lookup table: Redis command -> key position, read/write, dangerous.
 * By default the first argument is treated as the key, except for commands in the {@link #NO_KEY} list.
 */
final class CommandCatalog {

    enum KeyMode {
        /** No key to track. */
        NONE,
        /** First argument (index 1). */
        FIRST,
        /** Every argument after the command name is a key (DEL, MGET, EXISTS...). */
        ALL_ARGS,
        /** Odd indexes 1,3,5... are keys (MSET, MSETNX). */
        EVEN_ARGS,
        /** EVAL/EVALSHA: script, numkeys, key... */
        EVAL,
        /** Every argument except the last (BLPOP, BRPOP: key... timeout). */
        ALL_BUT_LAST
    }

    private static final Map<String, KeyMode> SPECIAL = new HashMap<String, KeyMode>();
    private static final Set<String> NO_KEY = set(
            "AUTH", "HELLO", "SELECT", "PING", "ECHO", "QUIT", "INFO", "CLIENT", "CONFIG", "COMMAND", "DBSIZE",
            "TIME", "ROLE", "RESET", "READONLY", "READWRITE", "WAIT", "WAITAOF", "LASTSAVE", "SAVE", "BGSAVE",
            "BGREWRITEAOF", "FLUSHDB", "FLUSHALL", "SWAPDB", "MULTI", "EXEC", "DISCARD", "UNWATCH", "SCRIPT",
            "FUNCTION", "SLOWLOG", "MEMORY", "LATENCY", "MODULE", "CLUSTER", "SENTINEL", "REPLICAOF", "SLAVEOF",
            "PSYNC", "SYNC", "MONITOR", "DEBUG", "SHUTDOWN", "FAILOVER", "ACL", "LOLWUT", "ASKING", "RANDOMKEY",
            "KEYS", "SCAN", "PUBLISH", "SPUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE",
            "PUNSUBSCRIBE", "SUNSUBSCRIBE", "PUBSUB", "OBJECT", "WATCH", "XINFO", "XGROUP");
    private static final Set<String> DANGEROUS = set("KEYS", "FLUSHALL", "FLUSHDB", "DEBUG", "MONITOR", "SAVE", "SHUTDOWN");
    private static final Set<String> READ = set(
            "GET", "MGET", "GETRANGE", "STRLEN", "GETBIT", "BITCOUNT", "BITPOS", "SUBSTR", "EXISTS", "TTL", "PTTL",
            "EXPIRETIME", "PEXPIRETIME", "TYPE", "DUMP", "HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN",
            "HEXISTS", "HSTRLEN", "HSCAN", "HRANDFIELD", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SRANDMEMBER",
            "SSCAN", "SINTER", "SUNION", "SDIFF", "ZRANGE", "ZRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGE",
            "ZREVRANGEBYSCORE", "ZREVRANGEBYLEX", "ZSCORE", "ZMSCORE", "ZCARD", "ZRANK", "ZREVRANK", "ZCOUNT",
            "ZLEXCOUNT", "ZSCAN", "ZRANDMEMBER", "LRANGE", "LLEN", "LINDEX", "LPOS", "XRANGE", "XREVRANGE", "XLEN",
            "XREAD", "PFCOUNT", "GEOPOS", "GEODIST", "GEOHASH", "GEORADIUS_RO", "GEOSEARCH", "STRALGO", "LCS");

    static {
        SPECIAL.put("DEL", KeyMode.ALL_ARGS);
        SPECIAL.put("UNLINK", KeyMode.ALL_ARGS);
        SPECIAL.put("EXISTS", KeyMode.ALL_ARGS);
        SPECIAL.put("MGET", KeyMode.ALL_ARGS);
        SPECIAL.put("TOUCH", KeyMode.ALL_ARGS);
        SPECIAL.put("MSET", KeyMode.EVEN_ARGS);
        SPECIAL.put("MSETNX", KeyMode.EVEN_ARGS);
        SPECIAL.put("EVAL", KeyMode.EVAL);
        SPECIAL.put("EVALSHA", KeyMode.EVAL);
        SPECIAL.put("EVAL_RO", KeyMode.EVAL);
        SPECIAL.put("EVALSHA_RO", KeyMode.EVAL);
        SPECIAL.put("BLPOP", KeyMode.ALL_BUT_LAST);
        SPECIAL.put("BRPOP", KeyMode.ALL_BUT_LAST);
    }

    private CommandCatalog() {
    }

    private static Set<String> set(String... values) {
        return new HashSet<String>(Arrays.asList(values));
    }

    static KeyMode modeOf(String upperCommand) {
        KeyMode special = SPECIAL.get(upperCommand);
        if (special != null) {
            return special;
        }
        return NO_KEY.contains(upperCommand) ? KeyMode.NONE : KeyMode.FIRST;
    }

    static boolean isRead(String upperCommand) {
        return READ.contains(upperCommand);
    }

    /** Shared with the tracker to count dangerous commands. */
    static boolean isDangerous(String upperCommand) {
        return DANGEROUS.contains(upperCommand);
    }
}
