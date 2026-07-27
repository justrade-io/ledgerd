package com.adbe.testkit;

import com.adbe.protocol.CommandEnvelopeEncoder;
import com.adbe.protocol.CommandResultDecoder;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.MessageHeaderEncoder;
import com.adbe.protocol.StatusCode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Minimal Aeron Cluster client used only by integration tests to drive the real
 * {@code BalanceService} and match {@link com.adbe.protocol.CommandResult}
 * messages by command id. This is a test harness, not the shipped Edge SDK.
 */
public final class ClusterTestClient implements EgressListener, AutoCloseable {

    private final AeronCluster cluster;
    private final MediaDriver ownMediaDriver;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandResultDecoder resultDecoder = new CommandResultDecoder();

    private boolean received;
    private long lastCommandIdLo = Long.MIN_VALUE;
    private StatusCode lastStatus = StatusCode.NULL_VAL;
    private long lastBalance;
    private long lastAllowance;
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

    /** Encodes and reliably offers one command to the cluster. */
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
                .accountC(accountC);

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

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.templateId() != CommandResultDecoder.TEMPLATE_ID) {
            return;
        }
        resultDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        lastCommandIdLo = resultDecoder.commandIdLo();
        lastStatus = resultDecoder.status();
        lastBalance = resultDecoder.resultBalance();
        lastAllowance = resultDecoder.resultAllowance();
        received = true;
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
