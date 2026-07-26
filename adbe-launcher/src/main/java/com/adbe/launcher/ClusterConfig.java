package com.adbe.launcher;

import java.io.File;
import java.nio.file.Path;

/**
 * Endpoint and directory configuration for a single cluster node.
 *
 * <p>Defaults describe a single-node localhost cluster suitable for local runs
 * and integration tests. Multi-node deployments supply an explicit
 * {@code clusterMembers} string and a matching {@code nodeId}.
 *
 * <p>The {@code clusterMembers} field uses the Aeron member format:
 * {@code id,ingress,consensus,log,catchup,archive} entries separated by
 * {@code |}.
 */
public final class ClusterConfig {

    private final int nodeId;
    private final String clusterMembers;
    private final String aeronDirectoryName;
    private final File archiveDir;
    private final File clusterDir;
    private final String archiveControlChannel;
    private final String archiveControlResponseChannel;
    private final String ingressChannel;
    private final String replicationChannel;

    private ClusterConfig(
            final int nodeId,
            final String clusterMembers,
            final String aeronDirectoryName,
            final File archiveDir,
            final File clusterDir,
            final String archiveControlChannel,
            final String archiveControlResponseChannel,
            final String ingressChannel,
            final String replicationChannel) {
        this.nodeId = nodeId;
        this.clusterMembers = clusterMembers;
        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveDir = archiveDir;
        this.clusterDir = clusterDir;
        this.archiveControlChannel = archiveControlChannel;
        this.archiveControlResponseChannel = archiveControlResponseChannel;
        this.ingressChannel = ingressChannel;
        this.replicationChannel = replicationChannel;
    }

    /** Builds a single-node localhost configuration rooted at {@code baseDir}. */
    public static ClusterConfig singleNodeLocalhost(final int nodeId, final Path baseDir) {
        final int portBase = 20100 + (nodeId * 100);
        final String host = "localhost";
        final int ingressPort = portBase;
        final int consensusPort = portBase + 1;
        final int logPort = portBase + 2;
        final int catchupPort = portBase + 3;
        final int archivePort = portBase + 4;

        final String member = nodeId + ","
                + host + ":" + ingressPort + ","
                + host + ":" + consensusPort + ","
                + host + ":" + logPort + ","
                + host + ":" + catchupPort + ","
                + host + ":" + archivePort;

        return new ClusterConfig(
                nodeId,
                member,
                baseDir.resolve("driver").toString(),
                baseDir.resolve("archive").toFile(),
                baseDir.resolve("cluster").toFile(),
                "aeron:udp?endpoint=" + host + ":" + archivePort,
                "aeron:udp?endpoint=" + host + ":0",
                "aeron:udp?term-length=64k",
                "aeron:udp?endpoint=" + host + ":0");
    }

    public int nodeId() {
        return nodeId;
    }

    public String clusterMembers() {
        return clusterMembers;
    }

    public String aeronDirectoryName() {
        return aeronDirectoryName;
    }

    public File archiveDir() {
        return archiveDir;
    }

    public File clusterDir() {
        return clusterDir;
    }

    public String archiveControlChannel() {
        return archiveControlChannel;
    }

    public String archiveControlResponseChannel() {
        return archiveControlResponseChannel;
    }

    public String ingressChannel() {
        return ingressChannel;
    }

    public String replicationChannel() {
        return replicationChannel;
    }
}
