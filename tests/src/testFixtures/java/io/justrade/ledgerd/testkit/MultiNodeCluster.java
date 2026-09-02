package io.justrade.ledgerd.testkit;

import io.justrade.ledgerd.config.CoreConfig;
import io.justrade.ledgerd.launcher.ClusterConfig;
import io.justrade.ledgerd.launcher.ClusterNode;
import java.nio.file.Path;

/**
 * Test harness that launches an in-process multi-node LEDGERD cluster on localhost
 * and lets tests stop and restart individual nodes to exercise leader election,
 * failover, and catch-up replay.
 *
 * <p>This is test-only infrastructure, never a shipped component. Nodes share a
 * single {@code clusterMembers} string; each has its own directory tree under
 * {@code baseDir/node-<id>} so a restarted node can recover its own log.
 */
public final class MultiNodeCluster implements AutoCloseable {

    private final ClusterConfig[] configs;
    private final CoreConfig coreConfig;
    private final ClusterNode[] nodes;

    public MultiNodeCluster(final int nodeCount, final Path baseDir) {
        this(nodeCount, baseDir, CoreConfig.defaults());
    }

    public MultiNodeCluster(final int nodeCount, final Path baseDir, final CoreConfig coreConfig) {
        this.configs = ClusterConfig.multiNodeLocalhost(nodeCount, baseDir);
        this.coreConfig = coreConfig;
        this.nodes = new ClusterNode[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodes[i] = new ClusterNode(configs[i], coreConfig, true);
        }
    }

    /** Number of member slots in this cluster (including any currently stopped). */
    public int size() {
        return nodes.length;
    }

    /** Returns the running node with the given id, or {@code null} if stopped. */
    public ClusterNode node(final int nodeId) {
        return nodes[nodeId];
    }

    /** The client ingress endpoints string covering all members. */
    public String ingressEndpoints() {
        return ClusterConfig.ingressEndpoints(nodes.length);
    }

    /** The Aeron directory name of a specific node, for client attachment. */
    public String aeronDirectoryName(final int nodeId) {
        return configs[nodeId].aeronDirectoryName();
    }

    /** Stops the given node without deleting its archive or cluster state. */
    public void stopNode(final int nodeId) {
        final ClusterNode node = nodes[nodeId];
        if (node != null) {
            node.close();
            nodes[nodeId] = null;
        }
    }

    /**
     * Restarts a previously stopped node, preserving its prior state so it can
     * recover its log and catch up to the cluster.
     */
    public void restartNode(final int nodeId) {
        if (nodes[nodeId] != null) {
            throw new IllegalStateException("node " + nodeId + " is already running");
        }
        nodes[nodeId] = new ClusterNode(configs[nodeId], coreConfig, false);
    }

    @Override
    public void close() {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                try {
                    nodes[i].close();
                } catch (final RuntimeException ignored) {
                    // Best-effort teardown; continue closing the remaining nodes.
                }
                nodes[i] = null;
            }
        }
    }
}
