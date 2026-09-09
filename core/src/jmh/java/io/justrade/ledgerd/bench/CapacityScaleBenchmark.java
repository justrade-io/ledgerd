package io.justrade.ledgerd.bench;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.CommandEnvelopeEncoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Capacity-sweep micro-benchmarks backing the O(1) scaling claims in the
 * capacity-planning model (docs/CAPACITY-PLANNING.md, section 1).
 *
 * <p>{@link AccountScale} grows the account count while measuring map lookup and
 * credit dispatch: both must stay flat because every operation is a constant-time
 * hash lookup, independent of how many slots the map preallocates. The
 * {@code mapLookup} and {@code creditDispatch} names match the hot-path budget
 * in {@code jmhBudgetCheck}, so the CI gate fails if either cliffs at scale.
 *
 * <p>{@link AssetScale} sweeps the asset count to measure the last-asset cache
 * (ADR 0009): a warm single-asset lookup avoids the bucket resolution a
 * round-robin multi-asset lookup pays on every call.
 */
public class CapacityScaleBenchmark {

    @State(Scope.Thread)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public static class AccountScale {

        @Param({"100000", "500000", "1000000"})
        int accounts;

        private BalanceEngine engine;
        private CommandOutcome outcome;
        private CommandEnvelopeEncoder encoder;
        private MessageHeaderEncoder headerEncoder;
        private CommandEnvelopeDecoder decoder;
        private MessageHeaderDecoder headerDecoder;
        private UnsafeBuffer buffer;
        private long seq;

        @Setup(Level.Trial)
        public void setup() {
            // accountCapacity 2^21 fits 1M accounts under the 0.65 load factor.
            engine = new BalanceEngine(CoreConfig.of(1 << 21, 1 << 14, 8, 1 << 18, 1024), new CoreMetrics());
            outcome = new CommandOutcome();
            encoder = new CommandEnvelopeEncoder();
            headerEncoder = new MessageHeaderEncoder();
            decoder = new CommandEnvelopeDecoder();
            headerDecoder = new MessageHeaderDecoder();
            buffer = new UnsafeBuffer(new byte[256]);

            for (long account = 1; account <= accounts; account++) {
                encode(account, CommandType.CREDIT, account, 1_000L);
                engine.process(wrapDecoder(), outcome);
            }
            // Continue the sequence after the populate pass so the measured
            // dispatch stays non-duplicate within the dedup window.
            seq = accounts;
        }

        private void encode(final long sequence, final CommandType type, final long account, final long amount) {
            encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .clientId(1L)
                    .clientSeq(sequence)
                    .commandIdHi(0L)
                    .commandIdLo(sequence)
                    .commandType(type)
                    .accountA(account)
                    .accountB(0L)
                    .amount(amount)
                    .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                    .accountC(0L)
                    .assetId(0L);
        }

        private CommandEnvelopeDecoder wrapDecoder() {
            headerDecoder.wrap(buffer, 0);
            decoder.wrap(
                    buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
            return decoder;
        }

        @Benchmark
        public long mapLookup() {
            return engine.balances().rawGet(0L, 1L);
        }

        @Benchmark
        public boolean creditDispatch() {
            encode(++seq, CommandType.CREDIT, 1L, 1L);
            return engine.process(wrapDecoder(), outcome);
        }
    }

    @State(Scope.Thread)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(1)
    public static class AssetScale {

        @Param({"1", "8", "32", "128"})
        int assets;

        private BalanceEngine engine;
        private CommandOutcome outcome;
        private CommandEnvelopeEncoder encoder;
        private MessageHeaderEncoder headerEncoder;
        private CommandEnvelopeDecoder decoder;
        private MessageHeaderDecoder headerDecoder;
        private UnsafeBuffer buffer;
        private int cursor;

        @Setup(Level.Trial)
        public void setup() {
            engine = new BalanceEngine(CoreConfig.of(1 << 16, 1 << 14, 8, 1 << 18, 1024), new CoreMetrics());
            outcome = new CommandOutcome();
            encoder = new CommandEnvelopeEncoder();
            headerEncoder = new MessageHeaderEncoder();
            decoder = new CommandEnvelopeDecoder();
            headerDecoder = new MessageHeaderDecoder();
            buffer = new UnsafeBuffer(new byte[256]);

            // Touch every asset with one account so each asset bucket exists.
            for (long asset = 0; asset < assets; asset++) {
                encode(asset, CommandType.CREDIT, asset, 1L, 1_000L);
                engine.process(wrapDecoder(), outcome);
            }
        }

        private void encode(
                final long sequence, final CommandType type, final long asset, final long account, final long amount) {
            encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .clientId(1L)
                    .clientSeq(sequence)
                    .commandIdHi(0L)
                    .commandIdLo(sequence)
                    .commandType(type)
                    .accountA(account)
                    .accountB(0L)
                    .amount(amount)
                    .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                    .accountC(0L)
                    .assetId(asset);
        }

        private CommandEnvelopeDecoder wrapDecoder() {
            headerDecoder.wrap(buffer, 0);
            decoder.wrap(
                    buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
            return decoder;
        }

        @Benchmark
        public long assetLookupWarm() {
            // Always the same asset, so the last-asset cache stays hot.
            return engine.balances().rawGet(0L, 1L);
        }

        @Benchmark
        public long assetLookupCold() {
            // Round-robin across assets so every lookup misses the last-asset cache.
            final long asset = cursor++ & (assets - 1);
            return engine.balances().rawGet(asset, 1L) + asset;
        }
    }
}
