package io.justrade.ledgerd.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.ClusterTool;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.SnapshotHeaderDecoder;
import io.justrade.ledgerd.protocol.StatusCode;
import io.justrade.ledgerd.testkit.ClusterTestClient;
import io.justrade.ledgerd.testkit.MultiNodeCluster;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 0 proof for the read-node HA design (future ADR 0008): verifies the
 * Aeron Archive model that multi-archive failover relies on.
 *
 * <p>The design assumes that a read replica can follow the cluster from ANY
 * member's archive, not just a single pinned member, because:
 *
 * <ul>
 *   <li>every member records the committed consensus log (stream 100) to its own
 *       archive, so live-log following works from any member; and
 *   <li>a snapshot's {@code logPosition} is the cluster-global consensus log
 *       position (written from {@code cluster.logPosition()}), so snapshot
 *       freshness is comparable across archives even though recording ids are not.
 * </ul>
 *
 * <p>Snapshot stream layout discovered by this test: the LEDGERD service snapshot
 * is recorded on stream 106, prefixed with three cluster-schema framing records
 * (schema 111) before the LEDGERD {@code SnapshotHeader} (schema 1, template 10);
 * the consensus-module snapshot is a separate, all-cluster-schema recording on
 * stream 107. A loader must therefore SKIP the cluster framing to reach the
 * {@code SnapshotHeader}. (The read node's header validation only inspects the
 * first record and so rejects the service snapshot - a latent bug masked today
 * by live-log-from-0, ADR 0007.)
 *
 * <p>It boots a 3-node cluster, commits state, triggers a snapshot via the
 * leader, then attaches an Archive client to each member and reports what each
 * archive holds. Assertions (A) and (B) are the confident invariants; assertion
 * (C) is the Phase 0 question - whether followers also carry a service snapshot
 * at the same cluster-global position. The per-member report is printed
 * regardless of the outcome so a failure reads as a finding, not a broken test.
 *
 * <p>Tagged {@code cluster}: multi-node and timing-sensitive, run via the opt-in
 * {@code clusterTest} task, never wired into {@code check}.
 */
@Tag("cluster")
class ReadReplicaArchiveModelClusterTest {

    private static final int NODE_COUNT = 3;
    private static final int LOG_STREAM_ID = 100;

    // The LEDGERD service snapshot lives on 106 (prefixed with cluster framing);
    // the consensus-module snapshot is the all-cluster-schema recording on 107.
    private static final int SERVICE_SNAPSHOT_STREAM_ID = 106;
    private static final int CONSENSUS_SNAPSHOT_STREAM_ID = 107;
    private static final int REPLAY_STREAM_ID = 44;
    private static final int LIST_WINDOW = 500;
    private static final long RESULT_TIMEOUT_MS = 30_000L;
    private static final long LEADER_SNAPSHOT_AWAIT_MS = 30_000L;
    private static final long FOLLOWER_SNAPSHOT_AWAIT_MS = 15_000L;
    private static final long HEADER_REPLAY_TIMEOUT_MS = 10_000L;
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;
    private static final long REPLAY_IDLE_GAP_MS = 1_000L;
    private static final long POLL_SLEEP_MS = 250L;

    @Test
    @Timeout(240)
    void archiveModelHoldsAcrossMembers(@TempDir final Path baseDir) throws Exception {
        try (MultiNodeCluster cluster = new MultiNodeCluster(NODE_COUNT, baseDir);
                ClusterTestClient client = ClusterTestClient.withOwnMediaDriver(cluster.ingressEndpoints())) {

            // ClusterConfig.multiNodeLocalhost is a pure function of baseDir, so these
            // paths match the nodes MultiNodeCluster launched above. Held separately so
            // the test can reach each member's archive channel and cluster dir.
            final ClusterConfig[] configs = ClusterConfig.multiNodeLocalhost(NODE_COUNT, baseDir);

            // Commit some state so the log and any snapshot are non-empty.
            client.send(1L, 0L, 0L, 1L, CommandType.CREDIT, 100L, 0L, 0L, 500L);
            assertTrue(client.awaitResult(1L, RESULT_TIMEOUT_MS), "credit 100 result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());
            client.send(1L, 1L, 0L, 2L, CommandType.CREDIT, 200L, 0L, 0L, 300L);
            assertTrue(client.awaitResult(2L, RESULT_TIMEOUT_MS), "credit 200 result");
            assertEquals(StatusCode.SUCCESS, client.lastStatus());

            final int leader = client.leaderMemberId();
            assertTrue(leader >= 0, "leader must be known before triggering a snapshot");

            // Trigger a cluster snapshot through the leader's cluster dir.
            assertTrue(ClusterTool.snapshot(configs[leader].clusterDir(), System.out), "snapshot trigger accepted");

            // Attach an Archive client (its own media driver, like a read replica) and
            // probe every member's archive.
            try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                            .threadingMode(ThreadingMode.SHARED)
                            .dirDeleteOnStart(true)
                            .dirDeleteOnShutdown(true));
                    Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()))) {

                final MemberArchiveReport[] reports = new MemberArchiveReport[NODE_COUNT];
                for (int i = 0; i < NODE_COUNT; i++) {
                    final long awaitMs = (i == leader) ? LEADER_SNAPSHOT_AWAIT_MS : FOLLOWER_SNAPSHOT_AWAIT_MS;
                    reports[i] = probeMember(aeron, configs[i], awaitMs);
                }
                printReport(leader, reports);

                // (A) Every member records the consensus log (stream 100): a read node
                // can follow the live log from ANY member, not just the pinned one.
                for (int i = 0; i < NODE_COUNT; i++) {
                    assertTrue(
                            reports[i].logCount >= 1,
                            "member " + i + " must have a consensus log recording on stream " + LOG_STREAM_ID);
                }

                // (B) The leader holds a valid LEDGERD service snapshot at a real position.
                assertTrue(
                        reports[leader].snapshotLogPosition > 0,
                        "leader must have a service snapshot with logPosition > 0 on stream "
                                + SERVICE_SNAPSHOT_STREAM_ID);

                // (C) PHASE 0 QUESTION: do followers also carry a service snapshot, at
                // the SAME cluster-global logPosition as the leader? If this fails, the
                // report above shows the raw per-member data; the Tier-1 failover design
                // does not depend on follower snapshots (it follows the live log).
                for (int i = 0; i < NODE_COUNT; i++) {
                    if (i == leader) {
                        continue;
                    }
                    assertTrue(
                            reports[i].snapshotLogPosition > 0,
                            "Phase 0: follower " + i + " has no service snapshot on stream "
                                    + SERVICE_SNAPSHOT_STREAM_ID
                                    + " - followers may not snapshot on a leader-triggered snapshot");
                    assertEquals(
                            reports[leader].snapshotLogPosition,
                            reports[i].snapshotLogPosition,
                            "Phase 0: snapshot logPosition must be cluster-global (equal across members)");
                }
            }
        }
    }

    // --- Archive probing helpers -----------------------

    /** Polls one member's archive until a service snapshot appears or the deadline passes. */
    private static MemberArchiveReport probeMember(final Aeron aeron, final ClusterConfig config, final long awaitMs) {
        final MemberArchiveReport report = new MemberArchiveReport();
        final long deadline = System.currentTimeMillis() + awaitMs;
        while (System.currentTimeMillis() < deadline) {
            try (AeronArchive archive = connectArchive(aeron, config.archiveControlChannel())) {
                report.logCount = countRecordings(archive, LOG_STREAM_ID);
                report.serviceSnapshotCount = countRecordings(archive, SERVICE_SNAPSHOT_STREAM_ID);
                report.consensusSnapshotCount = countRecordings(archive, CONSENSUS_SNAPSHOT_STREAM_ID);
                final long position = newestServiceSnapshotLogPosition(archive, aeron);
                if (position >= 0) {
                    report.snapshotLogPosition = position;
                    return report;
                }
            } catch (final RuntimeException e) {
                // Member archive not reachable yet (or a transient replay failure); retry.
            }
            sleep();
        }
        return report;
    }

    private static AeronArchive connectArchive(final Aeron aeron, final String controlChannel) {
        return AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(controlChannel)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
    }

    private static int countRecordings(final AeronArchive archive, final int targetStreamId) {
        final int[] count = {0};
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == targetStreamId) {
                        count[0]++;
                    }
                };
        archive.listRecordings(0L, LIST_WINDOW, consumer);
        return count[0];
    }

    /**
     * Finds the newest LEDGERD service snapshot recording (stream 106) and returns
     * its cluster-global log position, or {@code -1} if none is loadable.
     * Candidates are tried newest-first.
     */
    private static long newestServiceSnapshotLogPosition(final AeronArchive archive, final Aeron aeron) {
        final long[] candidates = new long[64];
        final int[] count = {0};
        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == SERVICE_SNAPSHOT_STREAM_ID && count[0] < candidates.length) {
                        candidates[count[0]++] = recordingId;
                    }
                };
        archive.listRecordings(0L, LIST_WINDOW, consumer);
        Arrays.sort(candidates, 0, count[0]);
        for (int i = count[0] - 1; i >= 0; i--) {
            final long position = readSnapshotHeaderLogPosition(archive, aeron, candidates[i]);
            if (position >= 0) {
                return position;
            }
        }
        return -1L;
    }

    /**
     * Replays a service snapshot recording and reads its {@code SnapshotHeader}
     * log position. The recording is prefixed with cluster-schema framing
     * records, so fragments are skipped until the LEDGERD {@code SnapshotHeader}
     * (LEDGERD schema, template 10) appears. Returns {@code -1} if the recording
     * holds no service snapshot header.
     */
    private static long readSnapshotHeaderLogPosition(
            final AeronArchive archive, final Aeron aeron, final long recordingId) {
        final Subscription subscription = aeron.addSubscription("aeron:udp?endpoint=localhost:0", REPLAY_STREAM_ID);
        try {
            final String endpoint = awaitResolvedEndpoint(subscription);
            if (endpoint == null) {
                return -1L;
            }
            final String replayChannel = "aeron:udp?endpoint=" + endpoint;
            archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, replayChannel, REPLAY_STREAM_ID);

            final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
            final SnapshotHeaderDecoder snapshotHeader = new SnapshotHeaderDecoder();
            final long[] captured = {-1L};
            final FragmentHandler handler = (buffer, offset, length, header) -> {
                if (captured[0] >= 0) {
                    return;
                }
                messageHeader.wrap(buffer, offset);
                if (messageHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                        || messageHeader.templateId() != SnapshotHeaderDecoder.TEMPLATE_ID) {
                    return; // cluster-schema framing or another record; keep scanning
                }
                snapshotHeader.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeader.blockLength(),
                        messageHeader.version());
                captured[0] = snapshotHeader.logPosition();
            };

            final long deadline = System.currentTimeMillis() + HEADER_REPLAY_TIMEOUT_MS;
            long lastFragmentMs = System.currentTimeMillis();
            while (captured[0] < 0 && System.currentTimeMillis() < deadline) {
                if (subscription.poll(handler, 16) > 0) {
                    lastFragmentMs = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastFragmentMs > REPLAY_IDLE_GAP_MS) {
                    break; // replay exhausted without a SnapshotHeader
                } else {
                    Thread.onSpinWait();
                }
            }
            return captured[0];
        } finally {
            subscription.close();
        }
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        return null;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printReport(final int leader, final MemberArchiveReport[] reports) {
        final StringBuilder sb = new StringBuilder(256);
        sb.append('\n');
        sb.append("==== Phase 0 archive model report ====\n");
        for (int i = 0; i < reports.length; i++) {
            final MemberArchiveReport r = reports[i];
            sb.append("member ")
                    .append(i)
                    .append(i == leader ? " (leader)  " : " (follower) ")
                    .append("log(stream100)=")
                    .append(r.logCount)
                    .append(" serviceSnap(stream106)=")
                    .append(r.serviceSnapshotCount)
                    .append(" consensusSnap(stream107)=")
                    .append(r.consensusSnapshotCount)
                    .append(" serviceSnapshotLogPosition=")
                    .append(r.snapshotLogPosition)
                    .append('\n');
        }
        sb.append("======================================\n");
        System.out.print(sb);
    }

    /** Per-member archive findings collected by {@link #probeMember}. */
    private static final class MemberArchiveReport {
        int logCount;
        int serviceSnapshotCount;
        int consensusSnapshotCount;
        long snapshotLogPosition = -1L;
    }
}
