package io.justrade.ledgerd.bench;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.core.BatchOutcome;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.CommandEnvelopeEncoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
import io.justrade.ledgerd.protocol.TransferBatchEncoder;
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
 * Micro-benchmarks for the hot path: envelope decode, primitive map lookup,
 * single-command dispatch, and transfer-batch apply/decode. Run with
 * {@code -prof gc} to assert zero steady-state allocation per event.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BalanceEngineBenchmark {

    @Param({"16", "256"})
    int batchSize;

    private BalanceEngine engine;
    private CommandOutcome outcome;
    private BatchOutcome batchOutcome;
    private CommandEnvelopeEncoder encoder;
    private TransferBatchEncoder batchEncoder;
    private MessageHeaderEncoder headerEncoder;
    private CommandEnvelopeDecoder decoder;
    private TransferBatchDecoder batchDecoder;
    private MessageHeaderDecoder headerDecoder;
    private UnsafeBuffer buffer;
    private UnsafeBuffer batchBuffer;
    private long seq;
    private long batchSeq;

    @Setup(Level.Trial)
    public void setup() {
        engine = new BalanceEngine(CoreConfig.of(1 << 16, 1024, 8, 1024, 1024), new CoreMetrics());
        outcome = new CommandOutcome();
        batchOutcome = new BatchOutcome(engine.maxBatchSize());
        encoder = new CommandEnvelopeEncoder();
        batchEncoder = new TransferBatchEncoder();
        headerEncoder = new MessageHeaderEncoder();
        decoder = new CommandEnvelopeDecoder();
        batchDecoder = new TransferBatchDecoder();
        headerDecoder = new MessageHeaderDecoder();
        buffer = new UnsafeBuffer(new byte[256]);
        batchBuffer = new UnsafeBuffer(new byte[1 << 16]);

        // Pre-fund account 1 so every transfer leg succeeds.
        encode(0L, CommandType.CREDIT, 1L, Long.MAX_VALUE / 2);
        engine.process(wrapDecoder(), outcome);
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

    private void encodeBatch(final long sequence, final boolean linked) {
        batchEncoder
                .wrapAndApplyHeader(batchBuffer, 0, headerEncoder)
                .clientId(1L)
                .clientSeq(sequence)
                .batchIdHi(0L)
                .batchIdLo(sequence);
        final TransferBatchEncoder.LegsEncoder legs = batchEncoder.legsCount(batchSize);
        for (int i = 0; i < batchSize; i++) {
            final boolean linksNext = linked && i < batchSize - 1;
            legs.next().fromId(1L).toId(1000L + i).amount(1L).assetId(0L).linked(linksNext ? (short) 1 : (short) 0);
        }
    }

    private CommandEnvelopeDecoder wrapDecoder() {
        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }

    private TransferBatchDecoder wrapBatchDecoder() {
        headerDecoder.wrap(batchBuffer, 0);
        batchDecoder.wrap(
                batchBuffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return batchDecoder;
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

    @Benchmark
    public boolean batchApply() {
        // Fresh sequence each call keeps the batch non-duplicate.
        encodeBatch(++batchSeq, false);
        return engine.processBatch(wrapBatchDecoder(), batchOutcome);
    }

    @Benchmark
    public boolean linkedChainApply() {
        // A single all-or-nothing chain exercises the undo-recording path.
        encodeBatch(++batchSeq, true);
        return engine.processBatch(wrapBatchDecoder(), batchOutcome);
    }
}
