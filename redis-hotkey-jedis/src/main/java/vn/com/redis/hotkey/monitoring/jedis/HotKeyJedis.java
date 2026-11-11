package vn.com.redis.hotkey.monitoring.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;

/**
 * Jedis integration (3.6.x to 6.x): hooks at the socket level, so it can capture regular commands, pipelines, MULTI/EXEC and Lua
 * without changing business code. In essence, the created socket is wrapped so that commands written to the OutputStream are "sniffed".
 *
 * <p>Quickest way to use it, in place of {@code new JedisPool(cfg, host, port, timeout, password)}:
 * <pre>
 * JedisPool pool = HotKeyJedis.newPool(cfg, host, port, timeout, password);
 * </pre>
 * When no {@code HotKeyMonitor} is running, the pool behaves exactly like a regular pool (it only adds a thin wrapper).
 */
public final class HotKeyJedis {

    private HotKeyJedis() {
    }

    /** Equivalent to {@code new JedisPool(poolConfig, host, port, timeoutMs, password)} but with monitoring. */
    public static JedisPool newPool(GenericObjectPoolConfig<Jedis> poolConfig, String host, int port,
                                    int timeoutMs, String password) {
        DefaultJedisClientConfig.Builder b = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(timeoutMs)
                .socketTimeoutMillis(timeoutMs);
        if (password != null && !password.isEmpty()) {
            b.password(password);
        }
        JedisClientConfig clientConfig = b.build();
        return new JedisPool(poolConfig, new MonitoredSocketFactory(new HostAndPort(host, port), clientConfig),
                clientConfig);
    }

    /** Use when building your own JedisPool/JedisFactory with a custom {@link JedisClientConfig} (SSL, user, database...). */
    public static MonitoredSocketFactory socketFactory(HostAndPort hostAndPort, JedisClientConfig clientConfig) {
        return new MonitoredSocketFactory(hostAndPort, clientConfig);
    }
}
