package com.adbe.read;

import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.http.QueryHttpServer;
import com.adbe.read.projection.ReadModelService;
import com.adbe.read.query.ReadQueryGateway;

/**
 * A read-side node: a cluster follower hosting a {@link ReadModelService}, wired
 * to an HTTP query boundary through a lock-free {@link ReadQueryGateway}. Applies
 * the committed log to build a complete, eventually-consistent read model and
 * serves balance, allowance, and total-supply reads over HTTP.
 */
public final class ReadNode implements AutoCloseable {

    private final ReadQueryGateway gateway;
    private final ClusterNode node;
    private final QueryHttpServer httpServer;

    public ReadNode(
            final ClusterConfig clusterConfig,
            final CoreConfig coreConfig,
            final ReadServiceConfig readConfig,
            final boolean cleanStart) {
        this.gateway = new ReadQueryGateway(readConfig.requestRingCapacity(), readConfig.responseRingCapacity());
        this.node = new ClusterNode(
                clusterConfig,
                coreConfig,
                cleanStart,
                (config, metrics) -> new ReadModelService(config, metrics, gateway));
        this.httpServer = new QueryHttpServer(gateway, readConfig);
    }

    /** The bound HTTP port (useful when a port of 0 was requested in tests). */
    public int httpPort() {
        return httpServer.port();
    }

    public ReadQueryGateway gateway() {
        return gateway;
    }

    @Override
    public void close() {
        httpServer.close();
        node.close();
        gateway.close();
    }
}
