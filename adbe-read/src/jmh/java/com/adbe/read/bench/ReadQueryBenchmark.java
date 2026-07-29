package com.adbe.read.bench;

import com.adbe.collections.BalanceStore;
import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceEngine;
import com.adbe.read.query.QueryCodec;
import com.adbe.read.query.QueryType;
import com.adbe.telemetry.CoreMetrics;
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
 * request off the ring, look up the balance store, and encode the response. This
 * mirrors {@code ReadModelService.answerBalances} exactly and must be
 * allocation-free (verify with {@code -prof gc}).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReadQueryBenchmark {

    private static final int ACCOUNTS = 1 << 14;
    private static final int BATCH = 16;

    private BalanceEngine engine;
    private UnsafeBuffer singleRequest;
    private UnsafeBuffer batchRequest;
    private UnsafeBuffer response;

    @Setup(Level.Trial)
    public void setup() {
        engine = new BalanceEngine(CoreConfig.of(1 << 16, 1024, 8, 1024, 1024), new CoreMetrics());
        final BalanceStore balances = engine.balances();
        for (int i = 0; i < ACCOUNTS; i++) {
            balances.set(i, (i + 1) * 10L);
        }

        singleRequest = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        batchRequest = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        response = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);

        QueryCodec.encodeRequest(singleRequest, 1L, QueryType.BALANCE, new long[] {ACCOUNTS / 2}, 1);

        final long[] ids = new long[BATCH];
        for (int i = 0; i < BATCH; i++) {
            ids[i] = (i * 97) % ACCOUNTS;
        }
        QueryCodec.encodeRequest(batchRequest, 2L, QueryType.BATCH_BALANCE, ids, BATCH);
    }

    @Benchmark
    public int singleBalance() {
        return answer(singleRequest, 1);
    }

    @Benchmark
    public int batchBalance() {
        return answer(batchRequest, BATCH);
    }

    private int answer(final UnsafeBuffer request, final int count) {
        final BalanceStore balances = engine.balances();
        final long correlationId = QueryCodec.correlationId(request, 0);
        QueryCodec.beginResponse(response, correlationId, QueryType.BATCH_BALANCE, count);
        for (int i = 0; i < count; i++) {
            final long id = QueryCodec.operand(request, 0, i);
            final long balance = balances.rawGet(id);
            final boolean exists = balance != BalanceStore.MISSING;
            QueryCodec.putEntry(response, i, exists ? balance : 0L, exists);
        }
        return QueryCodec.responseLength(count);
    }
}
