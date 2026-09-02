package io.justrade.ledgerd.pipeline;

/**
 * Shared Aeron stream id for the domain event journal (ADR 0011). The journaler
 * publishes and records events on this stream; consumers replay it. Defined here
 * so the launcher (producer) and the read/consumer side agree without either
 * depending on the other.
 */
public final class EventJournalStreams {

    /** Aeron stream id the domain event journal is published and recorded on. */
    public static final int STREAM_ID = 108;

    private EventJournalStreams() {}
}
