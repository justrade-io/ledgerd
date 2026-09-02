package io.justrade.ledgerd.read.bench;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.core.BalanceEngine;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.QueryRequestDecoder;
import io.justrade.ledgerd.protocol.QueryRequestEncoder;
import io.justrade.ledgerd.protocol.QueryResponseEncoder;
import io.justrade.ledgerd.protocol.QueryStatusCode;
import io.justrade.ledgerd.protocol.QueryType;
import io.justrade.ledgerd.telemetry.CoreMetrics;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Measures the per-query work performed on the read service thread: decode a
 * {@code QueryRequest} off the wire, look up the balance store, and encode a
 * {@code QueryResponse}. This mirrors {@code QueryResponder.encodeBalance} and
 * must be allocation-free (verify with {@code -prof gc}).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReadQueryBenchmark {

    private static final int ACCOUNTS = 1 << 14;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryRequestEncoder requestEncoder = new QueryRequestEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final QueryRequestDecoder requestDecoder = new QueryRequestDecoder();
    private final QueryResponseEncoder responseEncoder = new QueryResponseEncoder();

    private BalanceEngine engine;
    private UnsafeBuffer request;
    private UnsafeBuffer response;
    private int requestLength;

    @Setup(Level.Trial)
    public void setup() {
        engine = new BalanceEngine(CoreConfig.of(1 << 16, 1024, 8, 1024, 1024), new CoreMetrics());
        final BalanceStore balances = engine.balances();
        for (int i = 0; i < ACCOUNTS; i++) {
            balances.set(0L, i, (i + 1) * 10L);
        }

        request = new UnsafeBuffer(new byte[256]);
        response = new UnsafeBuffer(new byte[1024]);

        requestEncoder
                .wrapAndApplyHeader(request, 0, headerEncoder)
                .requestId(1L)
                .queryType(QueryType.BALANCE)
                .assetId(0L)
                .accountId(ACCOUNTS / 2)
                .responseStreamId(301)
                .responseChannel("aeron:udp?endpoint=localhost:0");
        requestLength = MessageHeaderEncoder.ENCODED_LENGTH + requestEncoder.encodedLength();
    }

    @Benchmark
    public int singleBalance() {
        headerDecoder.wrap(request, 0);
        requestDecoder.wrap(
                request, MessageHeaderEncoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());

        final long assetId = requestDecoder.assetId();
        final long accountId = requestDecoder.accountId();
        final long balance = engine.balances().rawGet(assetId, accountId);
        final boolean exists = balance != BalanceStore.MISSING;

        responseEncoder
                .wrapAndApplyHeader(response, 0, headerEncoder)
                .requestId(requestDecoder.requestId())
                .queryType(QueryType.BALANCE)
                .status(exists ? QueryStatusCode.SUCCESS : QueryStatusCode.NOT_FOUND)
                .appliedPosition(0L)
                .assetId(assetId)
                .accountId(accountId)
                .balance(exists ? balance : 0L)
                .exists(exists ? (short) 1 : (short) 0);
        return MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
    }
}
