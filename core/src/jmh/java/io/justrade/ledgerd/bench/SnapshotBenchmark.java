package io.justrade.ledgerd.bench;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.core.CommandOutcome;
import io.justrade.ledgerd.persistence.SnapshotManager;
import io.justrade.ledgerd.protocol.CommandEnvelopeDecoder;
import io.justrade.ledgerd.protocol.CommandEnvelopeEncoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
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
 * Measures snapshot write and recovery-read cost separately, as required by the
 * PRD: write time affects runtime SLA, read time affects the recovery-time
 * objective. Records are streamed through an in-memory length-prefixed buffer,
 * mirroring how the cluster streams them to an Aeron publication / image.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class SnapshotBenchmark implements SnapshotManager.SnapshotSink {

    @Param({"1024", "16384"})
    private int accounts;

    private CoreConfig config;
    private BalanceEngine engine;
    private SnapshotManager writer;
    private SnapshotManager reader;
    private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();
    private int writePos;

    private UnsafeBuffer sourceBuffer;
    private int serializedLength;

    @Setup(Level.Trial)
    public void setup() {
        config = CoreConfig.of(1 << 21, 1 << 14, 8, 1 << 18, 1024);
        engine = new BalanceEngine(config, new CoreMetrics());
        populate(engine, accounts);

        writer = new SnapshotManager();
        reader = new SnapshotManager();

        // Pre-serialize once so the read benchmark measures pure recovery cost.
        writePos = 0;
        engine.writeSnapshot(writer, this, () -> {}, 0L);
        serializedLength = writePos;
        final byte[] serialized = new byte[serializedLength];
        writeBuffer.getBytes(0, serialized, 0, serializedLength);
        sourceBuffer = new UnsafeBuffer(serialized);
    }

    @Override
    public void accept(final DirectBuffer buffer, final int offset, final int length) {
        writeBuffer.putInt(writePos, length);
        writePos += Integer.BYTES;
        writeBuffer.putBytes(writePos, buffer, offset, length);
        writePos += length;
    }

    @Benchmark
    public int snapshotWrite() {
        writePos = 0;
        engine.writeSnapshot(writer, this, () -> {}, 0L);
        return writePos;
    }

    @Benchmark
    public BalanceEngine snapshotRead() {
        final BalanceEngine target = new BalanceEngine(config, new CoreMetrics());
        target.beginSnapshotLoad(reader);
        int offset = 0;
        while (offset < serializedLength && !reader.loadComplete()) {
            final int length = sourceBuffer.getInt(offset);
            offset += Integer.BYTES;
            reader.onRecord(sourceBuffer, offset);
            offset += length;
        }
        return target;
    }

    private static void populate(final BalanceEngine engine, final int accounts) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();
        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final CommandOutcome outcome = new CommandOutcome();

        for (int i = 1; i <= accounts; i++) {
            encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                    .clientId(i)
                    .clientSeq(0L)
                    .commandIdHi(0L)
                    .commandIdLo(i)
                    .commandType(CommandType.CREDIT)
                    .accountA(i)
                    .accountB(0L)
                    .amount(1_000L)
                    .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                    .accountC(0L)
                    .assetId(0L);
            headerDecoder.wrap(buffer, 0);
            decoder.wrap(
                    buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
            engine.process(decoder, outcome);
        }
    }
}
