package com.adbe.risk.feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Incremental money-flow graph built from {@code TransferEvent} edges (ADR 0012).
 * Each directed edge {@code from -> to} aggregates the total transferred amount.
 * Degree centrality (distinct in-neighbours + out-neighbours) is maintained live
 * as edges arrive; PageRank is computed on demand over a copied snapshot so the
 * iterative pass never runs on, or blocks, the event follower thread.
 *
 * <p>Single-writer: {@link #addEdge(long, long, long)} is called only on the
 * follower's agent thread. Snapshot reads ({@link #degreeCentrality(long)},
 * {@link #snapshot()}, {@link #pageRank(int, double)}) come from the HTTP thread
 * over concurrent, weakly-consistent structures.
 */
public final class TransferGraph {

    /** Default number of PageRank power-iterations. */
    public static final int DEFAULT_PAGERANK_ITERATIONS = 20;

    /** Default PageRank damping factor. */
    public static final double DEFAULT_DAMPING = 0.85;

    private final ConcurrentHashMap<Long, Node> nodes = new ConcurrentHashMap<>();
    private volatile long edgeCount;

    /** Adds (or reinforces) a transfer edge {@code from -> to} of {@code amount}. */
    public void addEdge(final long from, final long to, final long amount) {
        final Node source = nodes.computeIfAbsent(from, id -> new Node());
        final Node target = nodes.computeIfAbsent(to, id -> new Node());
        final boolean newOut = source.out.putIfAbsent(to, amount) == null;
        if (!newOut) {
            source.out.merge(to, amount, Long::sum);
        }
        final boolean newIn = target.in.putIfAbsent(from, amount) == null;
        if (!newIn) {
            target.in.merge(from, amount, Long::sum);
        }
        if (newOut || newIn) {
            source.centrality = source.out.size() + source.in.size();
            target.centrality = target.out.size() + target.in.size();
        }
        if (newOut) {
            edgeCount++;
        }
    }

    /** Live degree centrality (distinct in + out neighbours) for {@code account}. */
    public double degreeCentrality(final long account) {
        final Node node = nodes.get(account);
        return node == null ? 0.0 : node.centrality;
    }

    /** Number of distinct directed edges observed. */
    public long edgeCount() {
        return edgeCount;
    }

    /** Number of distinct accounts (graph nodes). */
    public int nodeCount() {
        return nodes.size();
    }

    /**
     * Computes PageRank over a snapshot of the current graph and returns a mapping
     * of account id to rank. Runs on the caller (HTTP) thread; the follower is not
     * blocked.
     */
    public Map<Long, Double> pageRank(final int iterations, final double damping) {
        final List<Long> ids = new ArrayList<>(nodes.keySet());
        final int n = ids.size();
        final Map<Long, Double> ranks = new HashMap<>(n * 2);
        if (n == 0) {
            return ranks;
        }
        final Map<Long, Integer> index = new HashMap<>(n * 2);
        for (int i = 0; i < n; i++) {
            index.put(ids.get(i), i);
        }
        // Snapshot out-adjacency into index space.
        final int[][] outTargets = new int[n][];
        final double base = 1.0 / n;
        double[] rank = new double[n];
        for (int i = 0; i < n; i++) {
            rank[i] = base;
            final Node node = nodes.get(ids.get(i));
            final List<Integer> targets = new ArrayList<>(node.out.size());
            for (final Long to : node.out.keySet()) {
                final Integer j = index.get(to);
                if (j != null) {
                    targets.add(j);
                }
            }
            final int[] arr = new int[targets.size()];
            for (int k = 0; k < arr.length; k++) {
                arr[k] = targets.get(k);
            }
            outTargets[i] = arr;
        }
        final double teleport = (1.0 - damping) / n;
        for (int iter = 0; iter < iterations; iter++) {
            final double[] next = new double[n];
            double dangling = 0.0;
            for (int i = 0; i < n; i++) {
                next[i] = teleport;
                if (outTargets[i].length == 0) {
                    dangling += rank[i];
                }
            }
            final double danglingShare = damping * dangling / n;
            for (int i = 0; i < n; i++) {
                final int[] targets = outTargets[i];
                if (targets.length > 0) {
                    final double share = damping * rank[i] / targets.length;
                    for (final int j : targets) {
                        next[j] += share;
                    }
                }
                next[i] += danglingShare;
            }
            rank = next;
        }
        for (int i = 0; i < n; i++) {
            ranks.put(ids.get(i), rank[i]);
        }
        return ranks;
    }

    /** A consistent-enough snapshot of nodes and edges for the dashboard. */
    public Snapshot snapshot() {
        final List<NodeView> nodeViews = new ArrayList<>(nodes.size());
        final List<Edge> edges = new ArrayList<>();
        for (final Map.Entry<Long, Node> entry : nodes.entrySet()) {
            final Node node = entry.getValue();
            nodeViews.add(new NodeView(entry.getKey(), node.centrality));
            for (final Map.Entry<Long, Long> out : node.out.entrySet()) {
                edges.add(new Edge(entry.getKey(), out.getKey(), out.getValue()));
            }
        }
        return new Snapshot(nodeViews, edges);
    }

    private static final class Node {
        private final ConcurrentHashMap<Long, Long> out = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, Long> in = new ConcurrentHashMap<>();
        private volatile double centrality;
    }

    /** Immutable view of one account in a graph snapshot. */
    public record NodeView(long account, double centrality) {}

    /** Immutable directed money-flow edge with aggregated amount. */
    public record Edge(long from, long to, long amount) {}

    /** Immutable graph snapshot for the dashboard. */
    public record Snapshot(List<NodeView> nodes, List<Edge> edges) {}
}
