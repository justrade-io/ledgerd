package com.adbe.risk.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class TransferGraphTest {

    @Test
    void degreeCentralityCountsDistinctNeighbours() {
        final TransferGraph graph = new TransferGraph();
        graph.addEdge(1L, 2L, 100L);
        graph.addEdge(1L, 3L, 50L);
        graph.addEdge(4L, 1L, 25L);
        // Reinforcing an existing edge must not change centrality.
        graph.addEdge(1L, 2L, 10L);

        assertEquals(3.0, graph.degreeCentrality(1L), "1 has out-neighbours {2,3} and in-neighbour {4}");
        assertEquals(1.0, graph.degreeCentrality(2L));
        assertEquals(3L, graph.edgeCount(), "three distinct directed edges");
        assertEquals(4, graph.nodeCount());
    }

    @Test
    void snapshotAggregatesEdgeAmounts() {
        final TransferGraph graph = new TransferGraph();
        graph.addEdge(1L, 2L, 100L);
        graph.addEdge(1L, 2L, 40L);

        final TransferGraph.Snapshot snapshot = graph.snapshot();
        assertEquals(1, snapshot.edges().size());
        final TransferGraph.Edge edge = snapshot.edges().get(0);
        assertEquals(1L, edge.from());
        assertEquals(2L, edge.to());
        assertEquals(140L, edge.amount(), "reinforced edge aggregates amounts");
    }

    @Test
    void pageRankConcentratesOnSink() {
        final TransferGraph graph = new TransferGraph();
        // 1 -> 3, 2 -> 3: node 3 is the money sink and should rank highest.
        graph.addEdge(1L, 3L, 10L);
        graph.addEdge(2L, 3L, 10L);

        final Map<Long, Double> ranks =
                graph.pageRank(TransferGraph.DEFAULT_PAGERANK_ITERATIONS, TransferGraph.DEFAULT_DAMPING);
        assertTrue(ranks.get(3L) > ranks.get(1L), "sink outranks source");
        assertTrue(ranks.get(3L) > ranks.get(2L), "sink outranks source");

        final double total =
                ranks.values().stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(1.0, total, 1e-6, "PageRank is a probability distribution");
    }

    @Test
    void emptyGraphPageRankIsEmpty() {
        assertTrue(new TransferGraph().pageRank(10, 0.85).isEmpty());
    }
}
