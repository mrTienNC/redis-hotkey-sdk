# redis-hotkey-sdk

Client-side Redis hot key detection. It adds no load on Redis. Runs on **Java 8 to 21**, **Spring Boot 2.0 to 3.x** and **Jedis 3.6 to 6.x**.

Exports to **Prometheus** (pull) or **td-agent/Fluentd** (file, stdout, http). You only declare the exporter name.

## Modules

| Module | Contents | Dependencies |
|---|---|---|
| `redis-hotkey-core` | Tracker, Space-Saving Top-K, sliding window, RESP parser, Prometheus and td-agent exporters | None (JDK only) |
| `redis-hotkey-jedis` | Socket-level hook for Jedis. Captures plain commands, pipelines, MULTI/EXEC and Lua | core, Jedis (`provided`) |
| `redis-hotkey-spring-boot-starter` | Auto-configuration, enabled with `redis.hotkey.enabled=true` | core, spring-boot-autoconfigure (`provided`) |

## How it works

Jedis has no listener API. The SDK wraps the `Socket` created by the pool, observes the outgoing command bytes and parses the RESP framing as a stream. Only the command name and the key positions it needs are copied. Values and passwords (`AUTH`) are never copied.

```
Jedis ──writes──> MonitoredSocket ──> Redis
                       │ (same bytes)
                       ▼
               RespCommandSniffer ──> HotKeyTracker (sampling, non-blocking offer)
                                           │ bounded queue, drops samples when full
                                           ▼
                                     Aggregator (1 daemon thread)
                         normalize key → Space-Saving → sliding window
                                           │ every publish-interval-seconds
                                           ▼
                            Prometheus /metrics   |   td-agent (file/stdout/http)
```

## Installation

1. Add the dependencies (the starter pulls in core):

```xml
<dependency>
    <groupId>io.github.mrtiennc</groupId>
    <artifactId>redis-hotkey-jedis</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>io.github.mrtiennc</groupId>
    <artifactId>redis-hotkey-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

2. Create the pool through the SDK instead of `new JedisPool(...)`. When no monitor is running, use a plain pool and behavior stays unchanged:

```java
if (HotKeyMonitor.global() != null) {
    pool = HotKeyJedis.newPool(poolConfig, host, port, timeoutMs, password);
} else {
    pool = new JedisPool(poolConfig, host, port, timeoutMs, password);
}
```

If you build your own `JedisClientConfig` (SSL, user, database), use `HotKeyJedis.socketFactory(hostAndPort, clientConfig)` and pass it to `new JedisPool(poolConfig, socketFactory, clientConfig)`.

3. Configure (disabled by default):

```properties
redis.hotkey.enabled=true
redis.hotkey.exporters=prometheus,td-agent      # just declare the names
redis.hotkey.sample-rate=0.1
```

## Configuration reference

| Property | Default | Meaning |
|---|---|---|
| `redis.hotkey.enabled` | `false` | Master switch. Must be enabled explicitly |
| `redis.hotkey.app-name` | `spring.application.name` | Value of the `app` label |
| `redis.hotkey.instance-id` | hostname | Instance label (used in td-agent JSON) |
| `redis.hotkey.sample-rate` | `0.1` | Sampling ratio in (0, 1]. Counts are scaled by 1/rate |
| `redis.hotkey.queue-capacity` | `8192` | Queue to the aggregator. Samples are dropped when full |
| `redis.hotkey.window-seconds` | `60` | Sliding window length |
| `redis.hotkey.bucket-seconds` | `10` | Length of each bucket |
| `redis.hotkey.publish-interval-seconds` | `10` | Snapshot and export interval |
| `redis.hotkey.top-n` | `20` | Number of patterns exported |
| `redis.hotkey.export-raw-keys` | `false` | Export raw keys (td-agent JSON only, **never** Prometheus) |
| `redis.hotkey.exporters` | empty | `prometheus`, `td-agent` (alias `fluentd`) |
| `redis.hotkey.normalizer-rules[i].regex/replacement` | empty | Extra normalization rules, applied before the defaults |
| `redis.hotkey.prometheus.port` | `9404` | `/metrics` port |
| `redis.hotkey.prometheus.bind-address` | `0.0.0.0` | |
| `redis.hotkey.prometheus.path` | `/metrics` | |
| `redis.hotkey.td-agent.mode` | `file` | `file` (tail), `stdout`, `http` (in_http) |
| `redis.hotkey.td-agent.file-path` | `logs/redis-hotkey.log` | Rotates to one `.1` copy above `file-max-bytes` (50 MB) |
| `redis.hotkey.td-agent.http-url` | `http://127.0.0.1:9880/redis.hotkey` | |

Default normalization: UUID becomes `{uuid}`, long hex strings (16+ chars) become `{hash}`, standalone numbers become `{id}` (so `user:123` becomes `user:{id}`, while `oauth2` and `v2` stay as they are).

## Runtime control

```java
HotKeyTracker t = HotKeyMonitor.global().getTracker();
t.setEnabled(false);        // switch off immediately, no restart
t.setSampleRate(0.01);      // reduce load
```

## Prometheus metrics

```
redis_hotkey_ops_per_second{app,pattern,type="read|write"}     # Top-N patterns only; series disappear when they leave the Top-N
redis_hotkey_total_ops_per_second{app,type}
redis_hotkey_commands_seen_total{app}
redis_hotkey_dangerous_commands_total{app,command}              # KEYS, FLUSHALL, FLUSHDB, DEBUG, MONITOR, SAVE, SHUTDOWN
redis_hotkey_tracker_{enabled,sample_rate,queue_size}
redis_hotkey_tracker_dropped_total
redis_hotkey_parse_errors_total
```

Each instance only sees its own traffic. Aggregate in Prometheus: `sum by (app, pattern) (redis_hotkey_ops_per_second)`.

## Custom exporters

Implement `HotKeyExporter` and `HotKeyExporterProvider`, and register the provider in `META-INF/services/vn.com.redis.hotkey.monitoring.export.HotKeyExporterProvider`. Then use its name in `redis.hotkey.exporters`.

## Building

Building the SDK needs **JDK 9+** (it uses `maven.compiler.release=8` so that Java 9+ APIs are rejected at compile time). The output bytecode is Java 8, so JDK 8 projects can use it.

```
mvn clean install                         # tests against Jedis 3.6.3
mvn clean test -Djedis.version=6.1.0      # tests against Jedis 6.1.0
```

Integration tests need Redis, by default at `127.0.0.1:16379` (override with `-Dhotkey.test.redis=host:port`). Without Redis these tests are skipped.

```
docker run -d --name hotkey-test-redis -p 16379:6379 redis:7-alpine
```

Overhead benchmark: `mvn test -pl redis-hotkey-jedis -am -Dtest=OverheadBenchmarkTest -Dhotkey.bench=true "-DargLine=-Dhotkey.bench=true"`

## Publishing to Maven Central

One-time setup:

1. Create an account at https://central.sonatype.com and register a **namespace**. The `groupId` must be inside a namespace you have verified. For a GitHub project use `io.github.<your-user>`; a custom reverse-domain such as `vn.com.redis` requires proving you own that domain with a DNS TXT record.
2. Generate a **user token** in the portal and put it in `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>TOKEN_USERNAME</username>
      <password>TOKEN_PASSWORD</password>
    </server>
  </servers>
</settings>
```

3. Create a GPG key, publish the public key (`gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>`) and make it available to `gpg` when signing.
4. In the root `pom.xml` fill in the `<developers>` entry (name and email). `github.user` and the `groupId` (`io.github.mrtiennc`) are already set.

Release:

```
mvn versions:set -DnewVersion=1.0.0 -DgenerateBackupPoms=false
mvn -Prelease clean deploy
```

The bundle is uploaded to the portal for validation. Review it there and press Publish (or set `autoPublish` to `true`). Published versions cannot be deleted or changed, so first run `mvn -Prelease clean verify` and check that the sources jar, javadoc jar and `.asc` signature files are produced for every module.

## Limitations

- Only traffic going through a pool created by the SDK is visible. Other pools (for example a `JedisConnectionFactory` created by `RedisTemplate`) need their own integration.
- `EVAL/EVALSHA`: only keys declared in `KEYS` are seen, not keys accessed dynamically inside the script.
- Pub/Sub: channels are not counted as keys. `SUBSCRIBE` is counted only as a command.
- Numbers are approximate (sampling and Space-Saving). Good for detecting hot keys, not for accounting.
- A connection's parser disables itself if it sees bytes that are not valid RESP (recorded in `parse_errors_total`); it never guesses.
- Bandwidth (value size) is not measured.

## License

Apache License 2.0. See [LICENSE](LICENSE).
