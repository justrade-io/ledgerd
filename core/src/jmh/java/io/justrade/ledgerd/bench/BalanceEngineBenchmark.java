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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Micro-benchmarks for the hot path: envelope decode, primitive map lookup, and
 * full command dispatch. Run with {@code -prof gc} to assert zero steady-state
 * allocation per event.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BalanceEngineBenchmark {

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
        engine = new BalanceEngine(CoreConfig.of(1 << 16, 1024, 8, 1024, 1024), new CoreMetrics());
        outcome = new CommandOutcome();
        encoder = new CommandEnvelopeEncoder();
        headerEncoder = new MessageHeaderEncoder();
        decoder = new CommandEnvelopeDecoder();
        headerDecoder = new MessageHeaderDecoder();
        buffer = new UnsafeBuffer(new byte[256]);
        encode(0L, CommandType.CREDIT, 1L, 100L);
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
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }

    @Benchmark
    public long decodeEnvelope() {
        final CommandEnvelopeDecoder d = wrapDecoder();
        return d.clientId() + d.clientSeq() + d.accountA() + d.amount();
    }

    @Benchmark
    public long mapLookup() {
        return engine.balances().rawGet(0L, 1L);
    }

    @Benchmark
    public boolean creditDispatch() {
        // Fresh sequence each call keeps the command non-duplicate.
        encode(++seq, CommandType.CREDIT, 1L, 1L);
        return engine.process(wrapDecoder(), outcome);
    }
}
