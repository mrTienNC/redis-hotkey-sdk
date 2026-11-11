package vn.com.redis.hotkey.monitoring.jedis;

import java.net.Socket;

import redis.clients.jedis.DefaultJedisSocketFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Extends {@link DefaultJedisSocketFactory} (instead of implementing JedisSocketFactory directly) because that interface
 * DIFFERS between Jedis 3.6.x (many abstract methods: getHost, setSoTimeout...) and 6.x (only createSocket).
 * The DefaultJedisSocketFactory class and its createSocket() method exist in both versions, so it is binary compatible.
 */
public final class MonitoredSocketFactory extends DefaultJedisSocketFactory {

    public MonitoredSocketFactory(HostAndPort hostAndPort, JedisClientConfig config) {
        super(hostAndPort, config);
    }

    @Override
    public Socket createSocket() throws JedisConnectionException {
        return new MonitoredSocket(super.createSocket());
    }
}
