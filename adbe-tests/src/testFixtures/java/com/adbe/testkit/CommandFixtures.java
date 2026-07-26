package com.adbe.testkit;

import com.adbe.protocol.CommandEnvelopeDecoder;
import com.adbe.protocol.CommandEnvelopeEncoder;
import com.adbe.protocol.CommandType;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes {@link com.adbe.protocol.CommandEnvelope} messages into a reusable
 * buffer and returns a wrapped decoder positioned at the message body, matching
 * how {@code BalanceService} decodes an incoming session message.
 *
 * <p>Test-only helper; not part of the shipped Edge SDK.
 */
public final class CommandFixtures {

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();

    /** Encodes a command and returns a decoder wrapped at its body. */
    public CommandEnvelopeDecoder encode(
            final long clientId,
            final long clientSeq,
            final long commandIdHi,
            final long commandIdLo,
            final CommandType type,
            final long accountA,
            final long accountB,
            final long accountC,
            final long amount) {

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
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

        headerDecoder.wrap(buffer, 0);
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }

    /** Copies the most recently encoded full message (header + body) into a new array. */
    public byte[] lastEncodedBytes() {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        final byte[] out = new byte[length];
        buffer.getBytes(0, out, 0, length);
        return out;
    }
}
