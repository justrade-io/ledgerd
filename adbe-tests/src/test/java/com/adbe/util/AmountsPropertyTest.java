package com.adbe.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class AmountsPropertyTest {

    @Property
    void addOverflowsMatchesMathAddExact(@ForAll final long a, @ForAll final long b) {
        boolean threw = false;
        try {
            Math.addExact(a, b);
        } catch (final ArithmeticException e) {
            threw = true;
        }
        assertEquals(threw, Amounts.addOverflows(a, b));
    }

    @Property
    void nonNegativeSumsNeverOverflowForModestValues(@ForAll("modest") final long a, @ForAll("modest") final long b) {
        assertFalse(Amounts.addOverflows(a, b));
    }

    @net.jqwik.api.Provide
    net.jqwik.api.Arbitrary<Long> modest() {
        return net.jqwik.api.Arbitraries.longs().between(0L, 1_000_000_000_000L);
    }
}
