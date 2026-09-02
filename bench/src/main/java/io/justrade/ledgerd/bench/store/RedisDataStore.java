package io.justrade.ledgerd.bench.store;

import io.justrade.ledgerd.bench.Op;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

/**
 * Redis backend: each account is an integer string counter. Credit and debit are
 * {@code INCRBY} / {@code DECRBY}; a transfer is a single atomic Lua script
 * ({@code DECRBY} the source then {@code INCRBY} the destination), so it commits as
 * one server-side step with no round-trip in between. Redis is in-memory with no
 * durability enabled by default - the opposite end of the durability spectrum from
 * the LEDGERD and Postgres backends, which the report calls out explicitly.
 *
 * <p>The container is provisioned on demand via Testcontainers; a run requires a
 * reachable Docker daemon.
 */
public final class RedisDataStore extends ThreadedDataStore {

    private static final String IMAGE = "redis:7-alpine";
    private static final int REDIS_PORT = 6379;
    private static final int SETUP_BATCH = 1000;

    // Atomic move of `amount` from KEYS[1] to KEYS[2]; no interleaving is possible.
    private static final String TRANSFER_LUA =
            "redis.call('DECRBY', KEYS[1], ARGV[1]); redis.call('INCRBY', KEYS[2], ARGV[1]); return 1";

    private final GenericContainer<?> container;
    private final JedisPool pool;
    private int accounts;

    public RedisDataStore(final int workers) {
        super("redis", workers);
        this.container = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(REDIS_PORT);
        container.start();
        this.pool = new JedisPool(container.getHost(), container.getMappedPort(REDIS_PORT));
    }

    private static String key(final long id) {
        return "acct:" + id;
    }

    @Override
    public void setup(final int accounts, final long initialBalance) {
        this.accounts = accounts;
        try (Jedis jedis = pool.getResource()) {
            jedis.flushDB();
            Pipeline pipeline = jedis.pipelined();
            for (int i = 1; i <= accounts; i++) {
                pipeline.set(key(i), Long.toString(initialBalance));
                if (i % SETUP_BATCH == 0) {
                    pipeline.sync();
                    pipeline = jedis.pipelined();
                }
            }
            pipeline.sync();
        }
    }

    @Override
    protected Worker createWorker() {
        return new RedisWorker(pool.getResource());
    }

    @Override
    public void verify(final long expectedSupply) {
        long actual = 0L;
        try (Jedis jedis = pool.getResource()) {
            for (int i = 1; i <= accounts; i++) {
                final String value = jedis.get(key(i));
                if (value != null) {
                    actual += Long.parseLong(value);
                }
            }
        }
        if (actual != expectedSupply) {
            throw new IllegalStateException("redis supply " + actual + " != expected " + expectedSupply);
        }
    }

    @Override
    public void close() {
        pool.close();
        container.stop();
    }

    /** Per-thread Jedis connection borrowed from the pool. */
    private static final class RedisWorker implements Worker {
        private final Jedis jedis;

        RedisWorker(final Jedis jedis) {
            this.jedis = jedis;
        }

        @Override
        public void execute(final Op op) {
            switch (op.type()) {
                case CREDIT -> jedis.incrBy(key(op.accountA()), op.amount());
                case DEBIT -> jedis.decrBy(key(op.accountA()), op.amount());
                case TRANSFER -> jedis.eval(
                        TRANSFER_LUA,
                        List.of(key(op.accountA()), key(op.accountB())),
                        List.of(Long.toString(op.amount())));
                default -> throw new IllegalArgumentException("unknown op type: " + op.type());
            }
        }

        @Override
        public void close() {
            jedis.close();
        }
    }
}
