package io.justrade.ledgerd.risk.http;

import io.justrade.ledgerd.risk.RiskScoringService;
import io.justrade.ledgerd.risk.RiskScoringService.AccountRisk;
import io.justrade.ledgerd.risk.RiskServiceConfig;
import io.justrade.ledgerd.risk.feature.TransferGraph;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Netty HTTP boundary for the risk service (ADR 0012), mirroring the read
 * service's {@code QueryHttpServer}. It serves the static dashboard and the JSON
 * endpoints that back it. All JSON is built by hand on the Netty thread from
 * published feature snapshots, never on the event follower thread.
 *
 * <p>Routes:
 *
 * <pre>
 *   GET  /               dashboard (HTML + JS)
 *   GET  /risk/scores    top-N account risk scores
 *   GET  /risk/graph     money-flow graph (nodes + centrality + PageRank, edges)
 *   GET  /healthz        follower health
 *   GET  /metrics        event and follower counters
 * </pre>
 */
public final class RiskHttpServer implements AutoCloseable {

    private static final int MAX_CONTENT_LENGTH = 1 << 16;
    private static final System.Logger LOG = System.getLogger(RiskHttpServer.class.getName());

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel serverChannel;

    public RiskHttpServer(
            final RiskScoringService service, final RiskServiceConfig config, final FollowerHealth health) {
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
                                .addLast(new RiskHttpHandler(service, config, health));
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
        workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    }

    /** Read-only health view of the underlying event follower for the dashboard. */
    public interface FollowerHealth {
        boolean isHealthy();

        long failovers();

        long appliedPosition();
    }

    /** Per-channel handler that serves the dashboard and its JSON endpoints. */
    private static final class RiskHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final RiskScoringService service;
        private final RiskServiceConfig config;
        private final FollowerHealth health;

        RiskHttpHandler(final RiskScoringService service, final RiskServiceConfig config, final FollowerHealth health) {
            this.service = service;
            this.config = config;
            this.health = health;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
            final boolean keepAlive = HttpUtil.isKeepAlive(request);
            if (!HttpMethod.GET.equals(request.method())) {
                writeJson(ctx, keepAlive, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
                return;
            }
            final String path = stripQuery(request.uri());
            switch (path) {
                case "/", "/index.html" -> writeHtml(ctx, keepAlive, DashboardHtml.HTML);
                case "/risk/scores" -> writeJson(ctx, keepAlive, HttpResponseStatus.OK, scoresJson());
                case "/risk/graph" -> writeJson(ctx, keepAlive, HttpResponseStatus.OK, graphJson());
                case "/healthz" -> {
                    final boolean ok = health.isHealthy();
                    writeJson(
                            ctx,
                            keepAlive,
                            ok ? HttpResponseStatus.OK : HttpResponseStatus.SERVICE_UNAVAILABLE,
                            healthJson(ok));
                }
                case "/metrics" -> writeJson(ctx, keepAlive, HttpResponseStatus.OK, metricsJson());
                default -> writeJson(ctx, keepAlive, HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
            }
        }

        private String scoresJson() {
            final List<AccountRisk> top = service.topScores(config.maxScoreRows());
            final StringBuilder sb = new StringBuilder(32 + top.size() * 96);
            sb.append("{\"scores\":[");
            for (int i = 0; i < top.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                final AccountRisk r = top.get(i);
                sb.append("{\"account\":")
                        .append(r.account())
                        .append(",\"score\":")
                        .append(fixed(r.score()))
                        .append(",\"zScore\":")
                        .append(fixed(r.zScore()))
                        .append(",\"centrality\":")
                        .append(fixed(r.centrality()))
                        .append(",\"txCount\":")
                        .append(r.txCount())
                        .append(",\"flagged\":")
                        .append(r.flagged())
                        .append('}');
            }
            sb.append("]}");
            return sb.toString();
        }

        private String graphJson() {
            final TransferGraph graph = service.graph();
            final TransferGraph.Snapshot snapshot = graph.snapshot();
            final Map<Long, Double> ranks =
                    graph.pageRank(TransferGraph.DEFAULT_PAGERANK_ITERATIONS, TransferGraph.DEFAULT_DAMPING);
            final List<TransferGraph.NodeView> nodes = snapshot.nodes();
            final List<TransferGraph.Edge> edges = snapshot.edges();
            final int edgeLimit = Math.min(edges.size(), config.maxGraphEdges());
            final StringBuilder sb = new StringBuilder(64 + nodes.size() * 64 + edgeLimit * 48);
            sb.append("{\"nodes\":[");
            for (int i = 0; i < nodes.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                final TransferGraph.NodeView node = nodes.get(i);
                final Double rank = ranks.get(node.account());
                sb.append("{\"account\":")
                        .append(node.account())
                        .append(",\"centrality\":")
                        .append(fixed(node.centrality()))
                        .append(",\"pageRank\":")
                        .append(fixed(rank == null ? 0.0 : rank))
                        .append('}');
            }
            sb.append("],\"edges\":[");
            for (int i = 0; i < edgeLimit; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                final TransferGraph.Edge edge = edges.get(i);
                sb.append("{\"from\":")
                        .append(edge.from())
                        .append(",\"to\":")
                        .append(edge.to())
                        .append(",\"amount\":")
                        .append(edge.amount())
                        .append('}');
            }
            sb.append("]}");
            return sb.toString();
        }

        private String healthJson(final boolean ok) {
            return "{\"status\":\"" + (ok ? "ok" : "degraded") + "\",\"appliedPosition\":" + health.appliedPosition()
                    + ",\"failovers\":" + health.failovers() + "}";
        }

        private String metricsJson() {
            return "{\"eventsProcessed\":" + service.eventsProcessed() + ",\"balanceChanges\":"
                    + service.balanceChanges() + ",\"transfers\":" + service.transfers() + ",\"holds\":"
                    + service.holds() + ",\"allowanceChanges\":" + service.allowanceChanges() + ",\"rejects\":"
                    + service.rejects() + ",\"scoredAccounts\":" + service.scoredAccounts() + ",\"healthy\":"
                    + health.isHealthy() + ",\"failovers\":" + health.failovers() + ",\"appliedPosition\":"
                    + health.appliedPosition() + "}";
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            LOG.log(
                    System.Logger.Level.WARNING,
                    "risk HTTP error on " + ctx.channel().remoteAddress(),
                    cause);
            ctx.close();
        }
    }

    /** Formats a double with three decimals for JSON. Edge code, so allocation is fine. */
    private static String fixed(final double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return "0.000";
        }
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static String stripQuery(final String uri) {
        final int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    private static void writeHtml(final ChannelHandlerContext ctx, final boolean keepAlive, final String html) {
        final byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        final FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=utf-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        send(ctx, keepAlive, response);
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
        send(ctx, keepAlive, response);
    }

    private static void send(
            final ChannelHandlerContext ctx, final boolean keepAlive, final FullHttpResponse response) {
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
