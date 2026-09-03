package io.justrade.ledgerd.testkit;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.justrade.ledgerd.protocol.CommandEnvelopeEncoder;
import io.justrade.ledgerd.protocol.CommandResultDecoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.protocol.TransferBatchEncoder;
import io.justrade.ledgerd.protocol.TransferBatchResultDecoder;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Minimal Aeron Cluster client used only by integration tests to drive the real
 * {@code BalanceService} and match {@link io.justrade.ledgerd.protocol.CommandResult}
 * messages by command id. This is a test harness, not the shipped Edge SDK.
 */
public final class ClusterTestClient implements EgressListener, AutoCloseable {

    private final AeronCluster cluster;
    private final MediaDriver ownMediaDriver;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final UnsafeBuffer batchBuffer = new UnsafeBuffer(new byte[1 << 16]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final TransferBatchEncoder batchEncoder = new TransferBatchEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandResultDecoder resultDecoder = new CommandResultDecoder();
    private final TransferBatchResultDecoder batchResultDecoder = new TransferBatchResultDecoder();

    private boolean received;
    private long lastCommandIdLo = Long.MIN_VALUE;
    private StatusCode lastStatus = StatusCode.NULL_VAL;
    private long lastBalance;
    private long lastAllowance;

    private boolean batchReceived;
    private long lastBatchIdLo = Long.MIN_VALUE;
    private int lastBatchLegCount;
    private StatusCode[] lastBatchStatuses = new StatusCode[0];
    private long[] lastBatchBalances = new long[0];
    private boolean[] lastBatchHasBalance = new boolean[0];

    private int leaderMemberId = -1;
    private int leaderChanges;

    public ClusterTestClient(final String aeronDirectoryName, final String ingressEndpoints) {
        this(aeronDirectoryName, ingressEndpoints, null);
    }

    private ClusterTestClient(
            final String aeronDirectoryName, final String ingressEndpoints, final MediaDriver ownMediaDriver) {
        this.ownMediaDriver = ownMediaDriver;
        this.cluster = AeronCluster.connect(new AeronCluster.Context()
                .egressListener(this)
                .aeronDirectoryName(aeronDirectoryName)
                .ingressChannel("aeron:udp")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(30))
                .ingressEndpoints(ingressEndpoints));
    }

    /**
     * Creates a client backed by its own embedded media driver, so it survives
     * the shutdown of any individual cluster node (required for fault-injection
     * tests that kill the leader).
     */
    public static ClusterTestClient withOwnMediaDriver(final String ingressEndpoints) {
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        try {
            return new ClusterTestClient(mediaDriver.aeronDirectoryName(), ingressEndpoints, mediaDriver);
        } catch (final RuntimeException e) {
            mediaDriver.close();
            throw e;
        }
    }

    /** Encodes and reliably offers one command (default asset {@code 0}) to the cluster. */
    public void send(
            final long clientId,
            final long clientSeq,
            final long commandIdHi,
            final long commandIdLo,
            final CommandType type,
            final long accountA,
            final long accountB,
            final long accountC,
            final long amount) {
        send(clientId, clientSeq, commandIdHi, commandIdLo, type, 0L, accountA, accountB, accountC, amount);
    }

    /** Encodes and reliably offers one command on a given asset to the cluster. */
    public void send(
            final long clientId,
            final long clientSeq,
            final long commandIdHi,
            final long commandIdLo,
            final CommandType type,
            final long assetId,
            final long accountA,
            final long accountB,
            final long accountC,
            final long amount) {

        envelopeEncoder
                .wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .commandIdHi(commandIdHi)
                .commandIdLo(commandIdLo)
                .commandType(type)
                .accountA(accountA)
                .accountB(accountB)
                .amount(amount)
                .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                .accountC(accountC)
                .assetId(assetId);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + envelopeEncoder.encodedLength();
        long result;
        do {
            result = cluster.offer(buffer, 0, length);
            if (result < 0) {
                cluster.pollEgress();
                Thread.onSpinWait();
            }
        } while (result < 0);
    }

    /** Encodes and reliably offers one transfer batch to the cluster. */
    public void sendBatch(
            final long clientId,
            final long clientSeq,
            final long batchIdHi,
            final long batchIdLo,
            final long[] fromIds,
            final long[] toIds,
            final long[] amounts,
            final long[] assetIds,
            final boolean[] linked) {

        batchEncoder
                .wrapAndApplyHeader(batchBuffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .batchIdHi(batchIdHi)
                .batchIdLo(batchIdLo);
        final TransferBatchEncoder.LegsEncoder legs = batchEncoder.legsCount(fromIds.length);
        for (int i = 0; i < fromIds.length; i++) {
            legs.next()
                    .fromId(fromIds[i])
                    .toId(toIds[i])
                    .amount(amounts[i])
                    .assetId(assetIds[i])
                    .linked(linked[i] ? (short) 1 : (short) 0);
        }

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + batchEncoder.encodedLength();
        long result;
        do {
            result = cluster.offer(batchBuffer, 0, length);
            if (result < 0) {
                cluster.pollEgress();
                Thread.onSpinWait();
            }
        } while (result < 0);
    }

    /** Polls egress until the result for {@code expectedCommandIdLo} arrives or timeout. */
    public boolean awaitResult(final long expectedCommandIdLo, final long timeoutMs) {
        received = false;
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            cluster.pollEgress();
            if (received && lastCommandIdLo == expectedCommandIdLo) {
                return true;
            }
            cluster.sendKeepAlive();
            Thread.onSpinWait();
        }
        return false;
    }

    /** Polls egress until the result for {@code expectedBatchIdLo} arrives or timeout. */
    public boolean awaitBatchResult(final long expectedBatchIdLo, final long timeoutMs) {
        batchReceived = false;
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            cluster.pollEgress();
            if (batchReceived && lastBatchIdLo == expectedBatchIdLo) {
                return true;
            }
            cluster.sendKeepAlive();
            Thread.onSpinWait();
        }
        return false;
    }

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        headerDecoder.wrap(buffer, offset);
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        if (headerDecoder.templateId() == CommandResultDecoder.TEMPLATE_ID) {
            resultDecoder.wrap(buffer, bodyOffset, headerDecoder.blockLength(), headerDecoder.version());
            lastCommandIdLo = resultDecoder.commandIdLo();
            lastStatus = resultDecoder.status();
            lastBalance = resultDecoder.resultBalance();
            lastAllowance = resultDecoder.resultAllowance();
            received = true;
            return;
        }

        if (headerDecoder.templateId() == TransferBatchResultDecoder.TEMPLATE_ID) {
            batchResultDecoder.wrap(buffer, bodyOffset, headerDecoder.blockLength(), headerDecoder.version());
            lastBatchIdLo = batchResultDecoder.batchIdLo();
            final TransferBatchResultDecoder.ResultsDecoder results = batchResultDecoder.results();
            lastBatchLegCount = results.count();
            ensureBatchArrays(lastBatchLegCount);
            for (int i = 0; i < lastBatchLegCount; i++) {
                results.next();
                lastBatchStatuses[i] = results.status();
                lastBatchHasBalance[i] = results.hasBalance() != 0;
                lastBatchBalances[i] = results.resultBalance();
            }
            batchReceived = true;
            return;
        }
    }

    @Override
    public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {
        this.leaderMemberId = leaderMemberId;
        this.leaderChanges++;
    }

    public StatusCode lastStatus() {
        return lastStatus;
    }

    public long lastBalance() {
        return lastBalance;
    }

    public long lastAllowance() {
        return lastAllowance;
    }

    /** Number of legs in the most recent transfer-batch result. */
    public int lastBatchLegCount() {
        return lastBatchLegCount;
    }

    public StatusCode lastBatchStatus(final int leg) {
        return lastBatchStatuses[leg];
    }

    public boolean lastBatchHasBalance(final int leg) {
        return lastBatchHasBalance[leg];
    }

    public long lastBatchBalance(final int leg) {
        return lastBatchBalances[leg];
    }

    private void ensureBatchArrays(final int legCount) {
        if (lastBatchStatuses.length < legCount) {
            lastBatchStatuses = new StatusCode[legCount];
            lastBatchBalances = new long[legCount];
            lastBatchHasBalance = new boolean[legCount];
        }
    }

    /** The member id of the current cluster leader as tracked by the client, or -1. */
    public int leaderMemberId() {
        final int fromCluster = cluster.leaderMemberId();
        return fromCluster >= 0 ? fromCluster : leaderMemberId;
    }

    /** Number of leader-change notifications received on this client's session. */
    public int leaderChanges() {
        return leaderChanges;
    }

    @Override
    public void close() {
        cluster.close();
        if (ownMediaDriver != null) {
            ownMediaDriver.close();
        }
    }
}
