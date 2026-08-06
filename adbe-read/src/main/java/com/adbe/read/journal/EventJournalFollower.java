package com.adbe.read.journal;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;

/**
 * Standalone follower of the domain event journal (ADR 0011). It runs its own
 * embedded media driver, connects to a cluster member's Archive, replays the
 * recorded event stream, and delivers decoded events to a
 * {@link DomainEventListener}. It never joins Raft and does not affect quorum.
 *
 * <p>Multi-archive failover (ADR 0008): configured with an ordered list of member
 * Archive endpoints, it follows the first reachable one and, on an Archive error
 * or a liveness timeout, fails over to the next (round-robin with backoff),
 * carrying its {@code (logPosition, eventIndex)} de-duplication high-water mark so
 * a re-followed prefix is idempotent.
 *
 * <p>Single-writer: one Agrona {@link Agent} thread owns all state; it connects,
 * polls the replay, and invokes the listener, so the listener sees events on a
 * single thread with no concurrency control.
 */
public final class EventJournalFollower implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final long STARTUP_CONNECT_TIMEOUT_MS = 10_000L;
    private static final long REPLAY_RECONNECT_BACKOFF_MS = 250L;
    private static final System.Logger LOG = System.getLogger(EventJournalFollower.class.getName());

    private final EventJournalConfig config;
    private final DomainEventListener listener;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final String[] channels;
    private final long messageTimeoutNs;
    private final AgentRunner agentRunner;

    private State state = State.CONNECTING;
    private int channelIndex;
    private AeronArchive archive;
    private EventJournalSubscriber subscriber;
    private long appliedPosition;
    private long hwmLogPosition = -1L;
    private int hwmEventIndex = -1;
    private long nextConnectMs;
    private long nextReplayReconnectMs;
    private long lastActivityMs;

    private volatile boolean healthy;
    private volatile long failovers;

    private enum State {
        CONNECTING,
        FOLLOWING,
        DEGRADED
    }

    public EventJournalFollower(final EventJournalConfig config, final DomainEventListener listener) {
        this.config = config;
        this.listener = listener;
        final List<String> configured = config.archiveControlChannels();
        this.channels = configured.toArray(new String[0]);
        this.messageTimeoutNs = TimeUnit.MILLISECONDS.toNanos(config.archiveMessageTimeoutMs());

        MediaDriver driver = null;
        Aeron aeronClient = null;
        AgentRunner runner = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(config.aeronDir())
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(config.aeronDir()));
            runner = new AgentRunner(new BackoffIdleStrategy(), this::onAgentError, null, new Agent() {
                @Override
                public int doWork() {
                    return doWorkCycle();
                }

                @Override
                public String roleName() {
                    return "adbe-event-follower";
                }
            });
            this.mediaDriver = driver;
            this.aeron = aeronClient;
            this.agentRunner = runner;
        } catch (final RuntimeException e) {
            closeQuietly(runner, aeronClient, driver);
            throw e;
        }

        AgentRunner.startOnThread(agentRunner);
        awaitInitialConnect();
    }

    /** Whether the follower is currently connected to an Archive and following. */
    public boolean isHealthy() {
        return healthy;
    }

    /** Number of Archive failovers performed. */
    public long failovers() {
        return failovers;
    }

    /** The Aeron log position consumed up to across all sources. */
    public long appliedPosition() {
        return appliedPosition;
    }

    // --- Agent loop ------------------------------------------------------

    private int doWorkCycle() {
        return switch (state) {
            case CONNECTING, DEGRADED -> attemptConnect();
            case FOLLOWING -> followCycle();
        };
    }

    private int attemptConnect() {
        final long now = System.currentTimeMillis();
        if (now < nextConnectMs) {
            return 0;
        }
        if (connectArchive()) {
            state = State.FOLLOWING;
            lastActivityMs = now;
            nextReplayReconnectMs = 0L;
            healthy = true;
            LOG.log(System.Logger.Level.INFO, "Event follower connected to archive: {0}", channels[channelIndex]);
            return 1;
        }
        advanceChannel();
        nextConnectMs = now + config.failoverBackoffMs();
        state = State.DEGRADED;
        healthy = false;
        return 0;
    }

    private int followCycle() {
        final long now = System.currentTimeMillis();
        ensureSubscriber();
        if (subscriber == null) {
            if (now - lastActivityMs > config.livenessTimeoutMs()) {
                failover();
            }
            return 0;
        }
        int fragments;
        try {
            fragments = subscriber.poll(FRAGMENT_LIMIT);
        } catch (final RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Event follower poll failed: {0}", e.getMessage());
            failover();
            return 0;
        }
        if (fragments > 0) {
            lastActivityMs = now;
            captureProgress();
        }
        if (subscriber.isReplayEnded()) {
            captureProgress();
            subscriber.close();
            subscriber = null;
            nextReplayReconnectMs = now + REPLAY_RECONNECT_BACKOFF_MS;
        }
        if (System.currentTimeMillis() - lastActivityMs > config.livenessTimeoutMs()) {
            LOG.log(System.Logger.Level.WARNING, "Event follower liveness timeout; failing over");
            failover();
            return fragments;
        }
        healthy = true;
        return fragments;
    }

    private void ensureSubscriber() {
        if (subscriber != null || archive == null || System.currentTimeMillis() < nextReplayReconnectMs) {
            return;
        }
        final EventJournalSubscriber candidate = new EventJournalSubscriber(
                archive, appliedPosition, config.localHost(), listener, hwmLogPosition, hwmEventIndex);
        if (candidate.connect()) {
            subscriber = candidate;
        } else {
            candidate.close();
        }
    }

    private void captureProgress() {
        if (subscriber == null) {
            return;
        }
        if (subscriber.lastPosition() > appliedPosition) {
            appliedPosition = subscriber.lastPosition();
        }
        hwmLogPosition = subscriber.hwmLogPosition();
        hwmEventIndex = subscriber.hwmEventIndex();
    }

    private void failover() {
        advanceChannel();
        closeSubscriber();
        closeArchive();
        failovers++;
        state = State.DEGRADED;
        nextConnectMs = System.currentTimeMillis() + config.failoverBackoffMs();
        healthy = false;
        LOG.log(
                System.Logger.Level.WARNING,
                "Event follower failing over to next archive: {0} (failovers={1})",
                channels[channelIndex],
                failovers);
    }

    private boolean connectArchive() {
        try {
            archive = AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false)
                    .messageTimeoutNs(messageTimeoutNs)
                    .controlRequestChannel(channels[channelIndex])
                    .controlResponseChannel("aeron:udp?endpoint=" + config.localHost() + ":0")
                    .controlRequestStreamId(config.archiveControlStreamId()));
            return true;
        } catch (final RuntimeException e) {
            archive = null;
            return false;
        }
    }

    private void advanceChannel() {
        channelIndex = (channelIndex + 1) % channels.length;
    }

    private void onAgentError(final Throwable throwable) {
        LOG.log(System.Logger.Level.ERROR, "Event follower agent error", throwable);
    }

    private void awaitInitialConnect() {
        final long deadline = System.currentTimeMillis() + STARTUP_CONNECT_TIMEOUT_MS;
        while (!healthy && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void closeSubscriber() {
        if (subscriber != null) {
            subscriber.close();
            subscriber = null;
        }
    }

    private void closeArchive() {
        if (archive != null) {
            try {
                archive.close();
            } catch (final RuntimeException ignored) {
                // Best-effort teardown; the source may already be dead.
            }
            archive = null;
        }
    }

    @Override
    public void close() {
        closeQuietly(agentRunner);
        closeSubscriber();
        closeArchive();
        closeQuietly(aeron, mediaDriver);
    }

    private static void closeQuietly(final AutoCloseable... resources) {
        for (final AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (final Exception ignored) {
                    // Best-effort cleanup.
                }
            }
        }
    }
}
