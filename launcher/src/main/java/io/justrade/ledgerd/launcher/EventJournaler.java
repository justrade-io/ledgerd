package io.justrade.ledgerd.launcher;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.justrade.ledgerd.pipeline.EventJournalRing;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.MessageHandler;

/**
 * Drains the domain event journal ring and records it to the local Archive
 * (ADR 0011). Runs on its own {@link AgentRunner} thread, off the single-writer
 * consensus thread, so its Aeron I/O never perturbs the hot path.
 *
 * <p>The service thread encodes events into an off-heap {@link EventJournalRing};
 * this agent reads them in batches and offers each to an
 * {@link ExclusivePublication} on stream {@link #STREAM_ID}, which the local
 * Archive records. Every member records its own event stream (ADR 0008 fact A),
 * so a consumer can follow any reachable member and deduplicate by
 * {@code (logPosition, eventIndex)}.
 */
public final class EventJournaler implements Agent, AutoCloseable {

    /** Aeron stream id for the recorded domain event journal. */
    public static final int STREAM_ID = io.justrade.ledgerd.pipeline.EventJournalStreams.STREAM_ID;

    private static final String CHANNEL = "aeron:ipc?term-length=64k";
    private static final int FRAGMENT_LIMIT = 64;
    private static final int MAX_OFFER_SPINS = 10_000;
    private static final System.Logger LOG = System.getLogger(EventJournaler.class.getName());

    private final EventJournalRing ring;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final ExclusivePublication publication;
    private final AgentRunner runner;
    private final MessageHandler handler = this::onEvent;

    private long dropped;

    /**
     * Connects an Aeron client and Archive control session on {@code aeronDirectoryName},
     * starts recording the event stream, and launches the draining agent thread.
     *
     * @param aeronDirectoryName the node's media driver directory
     * @param archiveContext     a template Archive client context (cloned here)
     * @param ring               the service's event journal ring to drain
     */
    public EventJournaler(
            final String aeronDirectoryName, final AeronArchive.Context archiveContext, final EventJournalRing ring) {
        this.ring = ring;
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
        try {
            this.archive =
                    AeronArchive.connect(archiveContext.clone().aeron(aeron).ownsAeronClient(false));
            this.archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
            this.publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        } catch (final RuntimeException e) {
            aeron.close();
            throw e;
        }
        this.runner = new AgentRunner(new BackoffIdleStrategy(), this::onError, null, this);
        AgentRunner.startOnThread(runner);
    }

    @Override
    public int doWork() {
        return ring.read(handler, FRAGMENT_LIMIT);
    }

    // Offers one drained record to the recorded publication. The record has been
    // consumed from the ring, so it must be delivered: retry on transient
    // back-pressure, drop (and count) only when the publication is gone.
    private void onEvent(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        int spins = 0;
        while (true) {
            final long result = publication.offer(buffer, index, length);
            if (result >= 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                dropped++;
                return;
            }
            if (++spins > MAX_OFFER_SPINS) {
                dropped++;
                return;
            }
            Thread.onSpinWait();
        }
    }

    private void onError(final Throwable throwable) {
        LOG.log(System.Logger.Level.WARNING, "event journaler error", throwable);
    }

    /** Number of event records dropped because the publication was unavailable. */
    public long dropped() {
        return dropped;
    }

    @Override
    public String roleName() {
        return "ledgerd-event-journaler";
    }

    @Override
    public void close() {
        runner.close();
        publication.close();
        archive.close();
        aeron.close();
    }
}
