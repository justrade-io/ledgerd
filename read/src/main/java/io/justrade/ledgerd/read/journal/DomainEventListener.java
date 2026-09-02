package io.justrade.ledgerd.read.journal;

import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;

/**
 * Sink for decoded domain events replayed from the journal (ADR 0011). All
 * methods default to no-ops so a consumer overrides only the events it cares
 * about (for example the AI risk substrate needs only balance changes and
 * transfers). Invoked on the follower's single agent thread; implementations
 * must not block it.
 *
 * <p>Every callback carries the {@code (logPosition, eventIndex)} dedup key and
 * the leader-assigned {@code timestamp}, so a stateless or sliding-window
 * consumer needs no other coordinate.
 */
public interface DomainEventListener {

    default void onBalanceChanged(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long accountId,
            long newBalance,
            long delta,
            EventCause cause) {}

    default void onReserved(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long accountId,
            long newAvailable,
            long newReserved) {}

    default void onCaptured(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long accountId,
            long newAvailable,
            long newReserved) {}

    default void onReleased(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long accountId,
            long newAvailable,
            long newReserved) {}

    default void onTransfer(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long fromAccount,
            long toAccount,
            long amount) {}

    default void onAllowanceChanged(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long ownerId,
            long delegateId,
            long newAllowance) {}

    default void onCommandRejected(
            long logPosition,
            long timestamp,
            int eventIndex,
            long assetId,
            long accountId,
            long amount,
            CommandType commandType,
            StatusCode reason) {}
}
