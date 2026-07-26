package com.adbe.testkit;

import com.adbe.core.BalanceEngine;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.CommandType;
import java.util.Random;

/**
 * Applies a deterministic pseudo-random command workload to a {@link BalanceEngine}.
 *
 * <p>Given the same seed and count, the exact same command sequence is produced,
 * which is what replay-determinism and snapshot round-trip tests rely on. Uses a
 * seeded {@link Random}; this lives in test code only, never in the core.
 */
public final class WorkloadGenerator {

    private static final CommandType[] TYPES = CommandType.values();
    private static final int CLIENTS = 4;
    private static final int ACCOUNTS = 8;

    private WorkloadGenerator() {}

    /** Runs {@code count} deterministic commands against {@code engine}. */
    public static void apply(final BalanceEngine engine, final long seed, final int count) {
        final Random random = new Random(seed);
        final CommandFixtures fixtures = new CommandFixtures();
        final CommandOutcome outcome = new CommandOutcome();
        final long[] nextSeq = new long[CLIENTS];
        long commandId = 0L;

        for (int i = 0; i < count; i++) {
            final int client = random.nextInt(CLIENTS);
            final CommandType type = pickType(random);
            final long a = 1L + random.nextInt(ACCOUNTS);
            final long b = 1L + random.nextInt(ACCOUNTS);
            final long c = 1L + random.nextInt(ACCOUNTS);
            final long amount = 1L + random.nextInt(1000);
            final long seq = nextSeq[client]++;
            commandId++;
            engine.process(fixtures.encode(client + 1L, seq, 0L, commandId, type, a, b, c, amount), outcome);
        }
    }

    private static CommandType pickType(final Random random) {
        CommandType type = TYPES[random.nextInt(TYPES.length)];
        // NULL_VAL is a codec sentinel, never a real command.
        while (type == CommandType.NULL_VAL) {
            type = TYPES[random.nextInt(TYPES.length)];
        }
        return type;
    }
}
