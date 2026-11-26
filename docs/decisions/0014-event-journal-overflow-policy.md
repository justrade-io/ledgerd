# 0014 - Event Journal Overflow Policy

Status: Accepted
Date: 2026-11-26

## Context

The domain event journal (ADR [0011](0011-domain-event-journal.md)) emits
semantic facts on a dedicated off-heap ring, drained by a separate journaler
thread and recorded to the Archive. The ring is an Agrona `OneToOneRingBuffer`:
the single-writer consensus thread writes records with `EventJournalRing.write`,
which returns `false` rather than blocking when the ring is full.

Until now the service reacted to a full ring in exactly one way: drop the event
and increment an overflow counter (`BalanceService.offerEvent`). ADR 0011 noted
a deferred follow-up: whether the journal should instead be lossless under
sustained backpressure. This ADR resolves what the service does on overflow and
what it deliberately does not do.

Three facts constrain the choice:

1. The consensus thread is single-writer and must never block. Stalling it on a
   full journal ring would stall consensus and violate the hot-path rules.
2. The journal is a side output. Dropping, delaying, or duplicating a journal
   event does not change the deterministic state machine: balances, allowances,
   command results, and snapshots are identical regardless of journal outcome.
   Downstream consumers already deduplicate by `(logPosition, eventIndex)`.
3. True losslessness requires refusing or throttling ingress at the cluster
   boundary before a command is applied, which the `ClusteredService` ingress
   callback cannot do once a message has been delivered.

## Decision

Introduce a `CoreConfig.JournalOverflowPolicy` with two values, defaulting to
`DROP`:

- **`DROP`** (default): on a full ring, drop the event and increment
  `ledgerd_event_journal_overflow`. Zero hot-path impact. This preserves the
  existing behaviour, so no deployment changes semantics on upgrade.
- **`BLOCK`**: on a full ring, spin a bounded number of times
  (`Thread.onSpinWait`) to let the journaler drain, retrying the write; if the
  ring is still full after the bound, drop and increment the counter as a
  fail-safe. This trades a bounded amount of tail latency under journal
  saturation for best-effort completeness. It never blocks consensus.

The policy is selected with `-Dledgerd.journalOverflowPolicy=DROP|BLOCK` and only
has effect when the journal is enabled.

`BLOCK` is explicitly best-effort, not unconditional losslessness. Guaranteeing
that no event is ever dropped requires propagating backpressure to cluster
ingress (reject or throttle commands before they are applied). That remains a
deferred follow-up; it is a larger change to the ingress path and is out of scope
here.

## Consequences

- Determinism is unaffected by the policy: it only changes whether a side-output
  event is dropped, never the replicated state.
- Operators who need stronger journal completeness can opt into `BLOCK`, paying
  tail-latency variance only while the journal is saturated (the journaler not
  keeping up), and must still tolerate drops under sustained overload.
- The right first response to a rising `ledgerd_event_journal_overflow` counter
  is to size `eventJournalCapacity` up or speed up the journaler's downstream,
  not to rely on `BLOCK`.
- A future ADR may add ingress backpressure for an unconditionally lossless,
  audit-grade journal; this ADR deliberately does not.
