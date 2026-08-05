package com.adbe.read.http;

import com.adbe.read.ReplicationHealth;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.query.QueryCodec;
import com.adbe.read.query.QueryType;
import com.adbe.read.query.ReadQueryGateway;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Netty HTTP boundary for the read service. It translates REST requests into
 * query-ring submissions and completes them asynchronously when the service
 * thread answers. JSON encoding and heap buffers are used here because this is
 * Edge code, never the deterministic core hot path.
 *
 * <p>Routes:
 *
 * <pre>
 *   GET  /balance/{id}
 *   POST /balances                 body: any JSON/text containing the account ids
 *   GET  /allowance/{owner}/{delegate}
 *   GET  /supply
 *   GET  /healthz
 *   GET  /metrics
 * </pre>
 */
public final class QueryHttpServer implements AutoCloseable {

    private static final int MAX_CONTENT_LENGTH = 1 << 16;
    private static final System.Logger LOG = System.getLogger(QueryHttpServer.class.getName());

    private final ReadQueryGateway gateway;
    private final ReadServiceConfig config;
    private final ReplicationHealth health;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel serverChannel;

    public QueryHttpServer(
            final ReadQueryGateway gateway, final ReadServiceConfig config, final ReplicationHealth health) {
        this.gateway = gateway;
        this.config = config;
        this.health = health;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();

        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel channel) {
                        channel.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH))
                                .addLast(new QueryHttpHandler(gateway, config, health));
                    }
                });

        this.serverChannel = bootstrap
                .bind(new InetSocketAddress(config.httpPort()))
                .syncUninterruptibly()
                .channel();
    }

    /** The actual bound port (useful when {@code httpPort} was 0 in tests). */
    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        serverChannel.close().syncUninterruptibly();
        // Await event-loop termination so in-flight requests and their scheduled
        // timeout tasks are not dropped mid-shutdown.
        workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }

    /** Per-channel request handler that bridges HTTP to the query ring. */
    private static final class QueryHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final ReadQueryGateway gateway;
        private final ReadServiceConfig config;
        private final ReplicationHealth health;

        QueryHttpHandler(
                final ReadQueryGateway gateway, final ReadServiceConfig config, final ReplicationHealth health) {
            this.gateway = gateway;
            this.config = config;
            this.health = health;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
            final boolean keepAlive = HttpUtil.isKeepAlive(request);
            final HttpMethod method = request.method();
            final String path = stripQuery(request.uri());
            final long assetId = assetParam(request.uri());

            try {
                if (HttpMethod.GET.equals(method)) {
                    handleGet(ctx, keepAlive, path, assetId);
                } else if (HttpMethod.POST.equals(method) && "/balances".equals(path)) {
                    handleBatch(ctx, keepAlive, request.content().toString(StandardCharsets.UTF_8), assetId);
                } else {
                    writeJson(ctx, keepAlive, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
                }
            } catch (final NumberFormatException e) {
                writeJson(ctx, keepAlive, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"invalid path or body\"}");
            }
        }

        private void handleGet(
                final ChannelHandlerContext ctx, final boolean keepAlive, final String path, final long assetId) {
            if ("/healthz".equals(path)) {
                final boolean ok = health.isHealthy();
                final String json = "{\"status\":\"" + (ok ? "ok" : "stale") + "\",\"appliedPosition\":"
                        + health.appliedPosition() + ",\"endpoint\":\"" + health.activeEndpoint() + "\",\"failovers\":"
                        + health.failovers() + "}";
                writeJson(ctx, keepAlive, ok ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE, json);
            } else if ("/metrics".equals(path)) {
                writeJson(ctx, keepAlive, HttpResponseStatus.OK, metricsJson());
            } else if ("/supply".equals(path)) {
                submitSupply(ctx, keepAlive, assetId);
            } else if (path.startsWith("/balance/")) {
                final long accountId = Long.parseLong(path.substring("/balance/".length()));
                submitSingleBalance(ctx, keepAlive, assetId, accountId);
            } else if (path.startsWith("/allowance/")) {
                final String rest = path.substring("/allowance/".length());
                final int slash = rest.indexOf('/');
                if (slash <= 0 || slash == rest.length() - 1) {
                    throw new NumberFormatException("allowance requires owner/delegate");
                }
                final long owner = Long.parseLong(rest.substring(0, slash));
                final long delegate = Long.parseLong(rest.substring(slash + 1));
                submitAllowance(ctx, keepAlive, assetId, owner, delegate);
            } else {
                writeJson(ctx, keepAlive, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
            }
        }

        private void submitSingleBalance(
                final ChannelHandlerContext ctx, final boolean keepAlive, final long assetId, final long id) {
            final long[] ids = {id};
            final long correlationId = gateway.submit(QueryType.BALANCE, assetId, ids, 1, (buffer, offset, length) -> {
                final boolean exists = QueryCodec.entryPresent(buffer, offset, 0);
                final long balance = QueryCodec.entryValue(buffer, offset, 0);
                writeJson(ctx, keepAlive, HttpResponseStatus.OK, balanceJson(id, exists, balance));
            });
            afterSubmit(ctx, keepAlive, correlationId);
        }

        private void handleBatch(
                final ChannelHandlerContext ctx, final boolean keepAlive, final String body, final long assetId) {
            final long[] ids = parseIds(body, config.maxBatchSize());
            if (ids.length == 0) {
                writeJson(ctx, keepAlive, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"no account ids\"}");
                return;
            }
            final int count = ids.length;
            final long correlationId =
                    gateway.submit(QueryType.BATCH_BALANCE, assetId, ids, count, (buffer, offset, length) -> {
                        final StringBuilder sb = new StringBuilder(32 + count * 48);
                        sb.append("{\"balances\":[");
                        for (int i = 0; i < count; i++) {
                            if (i > 0) {
                                sb.append(',');
                            }
                            final boolean exists = QueryCodec.entryPresent(buffer, offset, i);
                            final long balance = QueryCodec.entryValue(buffer, offset, i);
                            sb.append(balanceJson(ids[i], exists, balance));
                        }
                        sb.append("]}");
                        writeJson(ctx, keepAlive, HttpResponseStatus.OK, sb.toString());
                    });
            afterSubmit(ctx, keepAlive, correlationId);
        }

        private void submitAllowance(
                final ChannelHandlerContext ctx,
                final boolean keepAlive,
                final long assetId,
                final long owner,
                final long delegate) {
            final long[] operands = {owner, delegate};
            final long correlationId =
                    gateway.submit(QueryType.ALLOWANCE, assetId, operands, 2, (buffer, offset, length) -> {
                        final long allowance = QueryCodec.entryValue(buffer, offset, 0);
                        final String json = "{\"owner\":" + owner + ",\"delegate\":" + delegate + ",\"allowance\":"
                                + allowance + "}";
                        writeJson(ctx, keepAlive, HttpResponseStatus.OK, json);
                    });
            afterSubmit(ctx, keepAlive, correlationId);
        }

        private void submitSupply(final ChannelHandlerContext ctx, final boolean keepAlive, final long assetId) {
            final long correlationId =
                    gateway.submit(QueryType.TOTAL_SUPPLY, assetId, EMPTY, 0, (buffer, offset, length) -> {
                        final long supply = QueryCodec.entryValue(buffer, offset, 0);
                        writeJson(ctx, keepAlive, HttpResponseStatus.OK, "{\"totalSupply\":" + supply + "}");
                    });
            afterSubmit(ctx, keepAlive, correlationId);
        }

        /** Rejects on overload, otherwise schedules a timeout that resolves to 504. */
        private void afterSubmit(final ChannelHandlerContext ctx, final boolean keepAlive, final long correlationId) {
            if (correlationId == ReadQueryGateway.NO_CAPACITY) {
                writeJson(
                        ctx,
                        keepAlive,
                        HttpResponseStatus.SERVICE_UNAVAILABLE,
                        "{\"error\":\"read service overloaded\"}");
                return;
            }
            ctx.executor()
                    .schedule(
                            () -> {
                                if (gateway.cancel(correlationId)) {
                                    writeJson(
                                            ctx,
                                            keepAlive,
                                            HttpResponseStatus.GATEWAY_TIMEOUT,
                                            "{\"error\":\"read timed out\"}");
                                }
                            },
                            config.requestTimeoutMs(),
                            java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        private String metricsJson() {
            return "{\"submitted\":" + gateway.submitted() + ",\"completed\":" + gateway.completed()
                    + ",\"pending\":" + gateway.pendingCount() + ",\"overloads\":" + gateway.overloads()
                    + ",\"orphanResponses\":" + gateway.orphanResponses() + ",\"failovers\":" + health.failovers()
                    + ",\"integrityFailures\":" + health.integrityFailures() + "}";
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            // Edge boundary: surface codec/aggregator/write failures instead of
            // dropping them silently, then close the offending connection.
            LOG.log(
                    System.Logger.Level.WARNING,
                    "read HTTP error on " + ctx.channel().remoteAddress(),
                    cause);
            ctx.close();
        }
    }

    private static final long[] EMPTY = new long[0];

    private static String balanceJson(final long account, final boolean exists, final long balance) {
        if (exists) {
            return "{\"account\":" + account + ",\"exists\":true,\"balance\":" + balance + "}";
        }
        return "{\"account\":" + account + ",\"exists\":false}";
    }

    private static String stripQuery(final String uri) {
        final int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    /** Parses the optional {@code ?asset=} query parameter; defaults to asset 0. */
    private static long assetParam(final String uri) {
        final int q = uri.indexOf("asset=");
        if (q < 0) {
            return 0L;
        }
        int end = q + "asset=".length();
        final int start = end;
        while (end < uri.length() && uri.charAt(end) != '&') {
            end++;
        }
        if (end == start) {
            return 0L;
        }
        return Long.parseLong(uri.substring(start, end));
    }

    /** Extracts up to {@code max} signed decimal integers from arbitrary text. */
    private static long[] parseIds(final String body, final int max) {
        final long[] scratch = new long[max];
        int count = 0;
        int i = 0;
        final int n = body.length();
        while (i < n && count < max) {
            char c = body.charAt(i);
            final boolean negative = c == '-' && i + 1 < n && Character.isDigit(body.charAt(i + 1));
            if (Character.isDigit(c) || negative) {
                int start = i;
                i++;
                while (i < n && Character.isDigit(body.charAt(i))) {
                    i++;
                }
                scratch[count++] = Long.parseLong(body.substring(start, i));
            } else {
                i++;
            }
        }
        final long[] result = new long[count];
        System.arraycopy(scratch, 0, result, 0, count);
        return result;
    }

    private static void writeJson(
            final ChannelHandlerContext ctx,
            final boolean keepAlive,
            final HttpResponseStatus status,
            final String json) {
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
