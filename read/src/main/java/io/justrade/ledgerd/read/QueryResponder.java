package io.justrade.ledgerd.read;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.QueryRequestDecoder;
import io.justrade.ledgerd.protocol.QueryResponseEncoder;
import io.justrade.ledgerd.protocol.QueryStatusCode;
import io.justrade.ledgerd.protocol.QueryType;
import io.justrade.ledgerd.read.config.ReadReplicaConfig;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Serves the read-side query protocol on a read replica's polling thread:
 * subscribes to {@link QueryRequestDecoder} frames, answers each from the
 * replica's replicated balance/allowance state on the same single thread that
 * advances replication, and publishes a {@link QueryResponseEncoder} to the
 * client's ephemeral response subscription.
 *
 * <p>Single-writer: {@link #poll()} must be called from the replica's polling
 * thread (the same thread that drives {@code ReadReplicaNode}), so the engine is
 * only ever touched by one thread. Responses are encoded into a preallocated
 * buffer with an exact per-element budget check; a batch response that would
 * overflow is truncated with status {@link QueryStatusCode#TRUNCATED} rather than
 * corrupting the buffer.
 */
public final class QueryResponder implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int RESPONSE_BUFFER_CAPACITY = 64 * 1024;
    private static final int MAX_RESPONSE_PUBLICATIONS = 64;
    private static final int RESPONSE_PUBLICATIONS_MASK = MAX_RESPONSE_PUBLICATIONS - 1;
    private static final int MAX_CHANNEL_LENGTH = 64;
    private static final int GROUP_SIZE_ENCODING_LENGTH = 4;

    /** Cap on the number of accounts a single batch-balance query may carry. */
    static final int MAX_BATCH_ACCOUNTS = 512;

    private static final byte[] UDP_CHANNEL_PREFIX = {'a', 'e', 'r', 'o', 'n', ':', 'u', 'd', 'p'};
    private static final byte[] IPC_CHANNEL_PREFIX = {'a', 'e', 'r', 'o', 'n', ':', 'i', 'p', 'c'};

    private final ReadReplicaNode replica;
    private final Aeron aeron;
    private final Subscription requests;
    private final FragmentAssembler requestAssembler;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final QueryRequestDecoder requestDecoder = new QueryRequestDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryResponseEncoder responseEncoder = new QueryResponseEncoder();
    private final UnsafeBuffer responseBuffer = new UnsafeBuffer(new byte[RESPONSE_BUFFER_CAPACITY]);
    private final long[] batchAccountIds = new long[MAX_BATCH_ACCOUNTS];
    private int batchAccountCount;
    private boolean batchTruncated;

    // Bounded LRU of response publications, keyed by a composite of the channel
    // bytes and stream id, so answering a query allocates nothing (a String
    // decode + concat key would churn per request). The channel bytes are
    // retained for collision-safe verification; the String form is decoded only
    // when a new publication must be created.
    private final long[] publicationKeys = new long[MAX_RESPONSE_PUBLICATIONS];
    private final byte[][] publicationChannels = new byte[MAX_RESPONSE_PUBLICATIONS][];
    private final Publication[] publicationSlots = new Publication[MAX_RESPONSE_PUBLICATIONS];
    private final byte[] channelScratch = new byte[MAX_CHANNEL_LENGTH];
    private int publicationHead;
    private int publicationCount;

    private long replies;
    private long dropped;
    private long received;

    /**
     * @param aeron the replica's Aeron client (already connected to the replica's
     *     media driver); the responder reuses it rather than opening a second one
     * @param replica the replica serving queries; must already be constructed
     * @param config replica configuration, whose query channel and stream id this
     *     responder subscribes to
     */
    public QueryResponder(final Aeron aeron, final ReadReplicaNode replica, final ReadReplicaConfig config) {
        this.replica = replica;
        this.aeron = aeron;
        this.requestAssembler = new FragmentAssembler(this::onRequest);
        this.requests = aeron.addSubscription(config.queryRequestChannel(), config.queryRequestStreamId());
    }

    /** Advances request delivery and reply publishing; call from the replica's polling thread. */
    public int poll() {
        return requests.poll(requestAssembler, FRAGMENT_LIMIT);
    }

    /** Number of queries answered. */
    public long replies() {
        return replies;
    }

    /** Number of responses dropped on publication backpressure; clients retry. */
    public long dropped() {
        return dropped;
    }

    /** Number of query requests received and decoded. */
    public long received() {
        return received;
    }

    private void onRequest(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                || headerDecoder.templateId() != QueryRequestDecoder.TEMPLATE_ID
                || length < MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
            return;
        }
        requestDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        received++;

        // SBE decodes groups and varData in schema order: the accountIds group
        // must be consumed before the trailing responseChannel varData is read.
        decodeAccountIds();

        final int channelLength = validChannelLength();
        if (channelLength == 0) {
            return;
        }
        // The uint32 stream id narrows to a Java int; a value whose top bit is set
        // is negative and invalid for Aeron, so reject it rather than open a
        // publication on a bogus stream.
        final int responseStreamId = (int) requestDecoder.responseStreamId();
        if (responseStreamId < 0) {
            return;
        }
        final int responseLength = encodeResponse();
        if (responseLength <= 0) {
            return;
        }
        final Publication publication = responsePublication(responseStreamId, channelLength);
        if (publication == null) {
            dropped++;
            return;
        }
        final long result = publication.offer(responseBuffer, 0, responseLength);
        if (result < 0) {
            dropped++;
        } else {
            replies++;
        }
    }

    /**
     * Copies the response channel into {@link #channelScratch} and returns its
     * length, or {@code 0} when the channel is empty, oversized, or not a
     * supported scheme. No String is allocated for validation.
     */
    private int validChannelLength() {
        final int length = requestDecoder.responseChannelLength();
        if (length == 0 || length > MAX_CHANNEL_LENGTH) {
            return 0;
        }
        requestDecoder.getResponseChannel(channelScratch, 0, length);
        return hasPrefix(UDP_CHANNEL_PREFIX, length) || hasPrefix(IPC_CHANNEL_PREFIX, length) ? length : 0;
    }

    private boolean hasPrefix(final byte[] prefix, final int length) {
        if (length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (channelScratch[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** A deterministic composite key of the channel bytes and stream id. */
    private long channelKey(final int length, final int streamId) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < length; i++) {
            hash ^= channelScratch[i] & 0xFF;
            hash *= 0x100000001b3L;
        }
        return (hash << 32) ^ (streamId & 0xFFFFFFFFL);
    }

    private boolean channelMatches(final int slot, final int length) {
        final byte[] stored = publicationChannels[slot];
        if (stored == null || stored.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (stored[i] != channelScratch[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] channelBytesCopy(final int length) {
        final byte[] copy = new byte[length];
        System.arraycopy(channelScratch, 0, copy, 0, length);
        return copy;
    }

    private int encodeResponse() {
        final QueryType type = requestDecoder.queryType();
        responseEncoder
                .wrapAndApplyHeader(responseBuffer, 0, headerEncoder)
                .requestId(requestDecoder.requestId())
                .queryType(type)
                .status(QueryStatusCode.SUCCESS)
                .appliedPosition(replica.appliedPosition());
        switch (type) {
            case BALANCE -> encodeBalance();
            case BATCH_BALANCE -> encodeBatchBalance();
            case ALLOWANCE -> encodeAllowance();
            case TOTAL_SUPPLY -> encodeTotalSupply();
            default -> responseEncoder.status(QueryStatusCode.UNSUPPORTED);
        }
        return MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
    }

    private void encodeBalance() {
        final long assetId = requestDecoder.assetId();
        final long accountId = requestDecoder.accountId();
        final boolean exists = replica.accountExists(assetId, accountId);
        responseEncoder
                .assetId(assetId)
                .accountId(accountId)
                .balance(replica.balance(assetId, accountId))
                .exists(exists ? (short) 1 : (short) 0);
        if (!exists) {
            responseEncoder.status(QueryStatusCode.NOT_FOUND);
        }
    }

    private void encodeBatchBalance() {
        final long assetId = requestDecoder.assetId();
        responseEncoder.assetId(assetId);

        boolean truncated = batchTruncated;
        final int fit = fitFixedElements(
                batchAccountCount, QueryResponseEncoder.BalancesEncoder.sbeBlockLength(), remainingBudget());
        if (fit < batchAccountCount) {
            truncated = true;
        }
        if (truncated) {
            responseEncoder.status(QueryStatusCode.TRUNCATED);
        }

        final QueryResponseEncoder.BalancesEncoder balances = responseEncoder.balancesCount(fit);
        for (int i = 0; i < fit; i++) {
            final long accountId = batchAccountIds[i];
            final boolean exists = replica.accountExists(assetId, accountId);
            balances.next()
                    .accountId(accountId)
                    .balance(replica.balance(assetId, accountId))
                    .exists(exists ? (short) 1 : (short) 0);
        }
    }

    /**
     * Consumes the request's {@code accountIds} group, advancing the decoder's
     * cursor past it so the trailing {@code responseChannel} varData is read at
     * the correct offset. Accounts beyond {@link #MAX_BATCH_ACCOUNTS} are still
     * skipped (to preserve the cursor) but flagged as truncated.
     */
    private void decodeAccountIds() {
        batchAccountCount = 0;
        batchTruncated = false;
        final QueryRequestDecoder.AccountIdsDecoder ids = requestDecoder.accountIds();
        while (ids.hasNext()) {
            ids.next();
            if (batchAccountCount < MAX_BATCH_ACCOUNTS) {
                batchAccountIds[batchAccountCount++] = ids.accountId();
            } else {
                batchTruncated = true;
            }
        }
    }

    private void encodeAllowance() {
        final long assetId = requestDecoder.assetId();
        final long ownerId = requestDecoder.ownerId();
        final long delegateId = requestDecoder.delegateId();
        responseEncoder
                .assetId(assetId)
                .ownerId(ownerId)
                .delegateId(delegateId)
                .allowance(replica.allowance(assetId, ownerId, delegateId));
    }

    private void encodeTotalSupply() {
        responseEncoder.assetId(requestDecoder.assetId()).totalSupply(replica.totalSupply(requestDecoder.assetId()));
    }

    private int remainingBudget() {
        return RESPONSE_BUFFER_CAPACITY - responseEncoder.limit();
    }

    private static int fitFixedElements(final int count, final int blockLength, final int budget) {
        final long needed = GROUP_SIZE_ENCODING_LENGTH + (long) count * blockLength;
        if (needed <= budget) {
            return count;
        }
        return Math.max(0, (budget - GROUP_SIZE_ENCODING_LENGTH) / blockLength);
    }

    // The channel bytes are still in channelScratch (single-threaded poll); the
    // String form is decoded only on a miss, when a publication must be created.
    private Publication responsePublication(final int streamId, final int channelLength) {
        final long key = channelKey(channelLength, streamId);
        for (int i = 0; i < publicationCount; i++) {
            final int index = (publicationHead + i) & RESPONSE_PUBLICATIONS_MASK;
            if (publicationKeys[index] == key && channelMatches(index, channelLength)) {
                if (i != publicationCount - 1) {
                    // Access-order LRU: move the hit to the tail so the oldest
                    // *used* entry is evicted when the cache is full.
                    final Publication publication = publicationSlots[index];
                    final long storedKey = publicationKeys[index];
                    final byte[] storedChannel = publicationChannels[index];
                    for (int j = i + 1; j < publicationCount; j++) {
                        final int src = (publicationHead + j) & RESPONSE_PUBLICATIONS_MASK;
                        final int dst = (publicationHead + j - 1) & RESPONSE_PUBLICATIONS_MASK;
                        publicationKeys[dst] = publicationKeys[src];
                        publicationChannels[dst] = publicationChannels[src];
                        publicationSlots[dst] = publicationSlots[src];
                    }
                    final int tail = (publicationHead + publicationCount - 1) & RESPONSE_PUBLICATIONS_MASK;
                    publicationKeys[tail] = storedKey;
                    publicationChannels[tail] = storedChannel;
                    publicationSlots[tail] = publication;
                }
                return publicationSlots[(publicationHead + publicationCount - 1) & RESPONSE_PUBLICATIONS_MASK];
            }
        }
        Publication publication;
        try {
            final String channel = new String(channelScratch, 0, channelLength, StandardCharsets.UTF_8);
            publication = aeron.addPublication(channel, streamId);
        } catch (final RuntimeException e) {
            // A malformed channel must not kill the poll loop; skip the reply.
            return null;
        }
        if (publicationCount == MAX_RESPONSE_PUBLICATIONS) {
            // Full: evict the oldest entry before inserting the newest.
            final int evicted = publicationHead;
            publicationSlots[evicted].close();
            publicationSlots[evicted] = null;
            publicationKeys[evicted] = 0L;
            publicationChannels[evicted] = null;
            publicationHead = (publicationHead + 1) & RESPONSE_PUBLICATIONS_MASK;
            publicationCount--;
        }
        final int slot = (publicationHead + publicationCount) & RESPONSE_PUBLICATIONS_MASK;
        publicationKeys[slot] = key;
        publicationChannels[slot] = channelBytesCopy(channelLength);
        publicationSlots[slot] = publication;
        publicationCount++;
        return publication;
    }

    @Override
    public void close() {
        for (int i = 0; i < publicationCount; i++) {
            final int index = (publicationHead + i) & RESPONSE_PUBLICATIONS_MASK;
            final Publication publication = publicationSlots[index];
            if (publication != null) {
                publication.close();
            }
        }
        publicationHead = 0;
        publicationCount = 0;
        requests.close();
    }
}
