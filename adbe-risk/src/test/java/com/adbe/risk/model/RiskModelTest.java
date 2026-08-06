package com.adbe.risk.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RiskModelTest {

    @Test
    void scoreIsBoundedAndMonotonic() {
        final RiskModel model = new RiskModel();
        final double low = model.score(0.0, 0.0);
        final double mid = model.score(2.0, 5.0);
        final double high = model.score(8.0, 40.0);

        assertEquals(0.0, low, "no signal scores zero");
        assertTrue(mid > low, "more signal scores higher");
        assertTrue(high > mid, "even more signal scores higher");
        assertTrue(high <= 1.0, "score is bounded at 1.0");
    }

    @Test
    void slowdownIsNotRisk() {
        final RiskModel model = new RiskModel();
        assertEquals(0.0, model.score(-5.0, 0.0), "a negative z-score (slowdown) adds no risk");
    }

    @Test
    void flaggingHonoursThreshold() {
        final RiskModel model = new RiskModel(1.0, 0.0, 4.0, 20.0, 0.5);
        assertFalse(model.isFlagged(model.score(1.0, 0.0)), "z=1 of ref 4 -> 0.25 < 0.5");
        assertTrue(model.isFlagged(model.score(4.0, 0.0)), "z=4 of ref 4 -> 1.0 >= 0.5");
    }
}
