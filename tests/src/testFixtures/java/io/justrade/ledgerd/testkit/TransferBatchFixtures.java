package io.justrade.ledgerd.testkit;

import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.TransferBatchDecoder;
import io.justrade.ledgerd.protocol.TransferBatchEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes a {@link io.justrade.ledgerd.protocol.TransferBatch} message into a
 * reusable buffer and returns a wrapped decoder positioned at the message body,
 * matching how {@code BalanceService} decodes an incoming session message.
 *
 * <p>Test-only helper; not part of the shipped Edge SDK.
 */
public final class TransferBatchFixtures {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1 << 16]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final TransferBatchEncoder encoder = new TransferBatchEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final TransferBatchDecoder decoder = new TransferBatchDecoder();

    /** Encodes a batch of transfer legs and returns a decoder wrapped at its body. */
    public TransferBatchDecoder encode(
            final long clientId,
            final long clientSeq,
            final long batchIdHi,
            final long batchIdLo,
            final long[] fromIds,
            final long[] toIds,
            final long[] amounts,
            final long[] assetIds,
            final boolean[] linked) {

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .clientId(clientId)
                .clientSeq(clientSeq)
                .batchIdHi(batchIdHi)
                .batchIdLo(batchIdLo);
        final TransferBatchEncoder.LegsEncoder legs = encoder.legsCount(fromIds.length);
        for (int i = 0; i < fromIds.length; i++) {
            legs.next()
                    .fromId(fromIds[i])
                    .toId(toIds[i])
                    .amount(amounts[i])
                    .assetId(assetIds[i])
                    .linked(linked[i] ? (short) 1 : (short) 0);
        }

        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }
}
