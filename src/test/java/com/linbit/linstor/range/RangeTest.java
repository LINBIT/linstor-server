package com.linbit.linstor.range;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTest
{
    private static Range r(int from, int to)
    {
        return new Range(from, to);
    }

    // ===============================================================
    // parseList
    // ===============================================================

    // ---------------------------------------------------------------
    // Empty / whitespace input
    // ---------------------------------------------------------------

    @Test
    public void parseListEmptyStringReturnsEmptyList()
    {
        assertEquals(List.of(), Range.parseList(""));
    }

    @Test
    public void parseNullStringReturnsEmptyList()
    {
        assertEquals(List.of(), Range.parseList(null));
    }

    @Test
    public void parseListWhitespaceOnlyReturnsEmptyList()
    {
        assertEquals(List.of(), Range.parseList("   "));
    }

    // ---------------------------------------------------------------
    // Single range
    // ---------------------------------------------------------------

    @Test
    public void parseListSingleRange()
    {
        assertEquals(List.of(r(5, 10)), Range.parseList("5-10"));
    }

    @Test
    public void parseListSingleRangeWithSpacesAroundDash()
    {
        assertEquals(List.of(r(5, 10)), Range.parseList("5 - 10"));
    }

    @Test
    public void parseListSingleRangeWithLeadingAndTrailingWhitespace()
    {
        assertEquals(List.of(r(5, 10)), Range.parseList("  5-10  "));
    }

    @Test
    public void parseListSingleNumberIsPointRange()
    {
        // "7000" is shorthand for "7000-7000"
        assertEquals(List.of(r(7000, 7000)), Range.parseList("7000"));
    }

    @Test
    public void parseListEqualEndpointsYieldPointRange()
    {
        assertEquals(List.of(r(7, 7)), Range.parseList("7-7"));
    }

    @Test
    public void parseListReversedFromAndToIsAccepted()
    {
        // the Range record's canonical constructor swaps if from > to,
        // so "10-5" should not throw
        assertEquals(List.of(r(5, 10)), Range.parseList("10-5"));
    }

    @Test
    public void parseNegative()
    {
        assertEquals(List.of(r(-1, 10)), Range.parseList("-1-10"));
        assertEquals(List.of(r(-1, 10)), Range.parseList("10--1"));
        assertEquals(List.of(r(-10, -1)), Range.parseList("-1--10"));
        assertEquals(List.of(r(-1, -1)), Range.parseList("-1"));
    }

    // ---------------------------------------------------------------
    // Multiple ranges (comma separated)
    // ---------------------------------------------------------------

    @Test
    public void parseListTwoCommaSeparatedRanges()
    {
        assertEquals(List.of(r(1, 5), r(10, 15)), Range.parseList("1-5,10-15"));
    }

    @Test
    public void parseListCommasWithWhitespace()
    {
        assertEquals(List.of(r(1, 5), r(10, 15)), Range.parseList("1 - 5 , 10 - 15"));
    }

    @Test
    public void parseListMixedRangesAndSinglePorts()
    {
        assertEquals(List.of(r(1, 5), r(8, 8), r(10, 15)), Range.parseList("1-5,8,10-15"));
    }

    @Test
    public void parseListPreservesInputOrder()
    {
        // only reorder if merging
        assertEquals(List.of(r(1, 5), r(20, 25)), Range.parseList("20-25,1-5"));
        assertEquals(List.of(r(20, 25), r(1, 5)), Range.parseList("20-25,1-5", false));
    }

    @Test
    public void parseListPreservesOverlappingEntries()
    {
        assertEquals(List.of(r(1, 15)), Range.parseList("1-10,5-15"));
        assertEquals(List.of(r(1, 10), r(5, 15)), Range.parseList("1-10,5-15", false));
    }

    @Test
    public void parseListPreservesDuplicateEntries()
    {
        assertEquals(List.of(r(3, 7)), Range.parseList("3-7,3-7"));
        assertEquals(List.of(r(3, 7), r(3, 7)), Range.parseList("3-7,3-7", false));
    }

    @Test
    public void parseListTcpPortAutoRangeStyleInput()
    {
        // realistic TcpPortAutoRange property value
        assertEquals(
            List.of(r(7000, 7099), r(7200, 7299), r(8000, 8000)),
            Range.parseList("7000-7099, 7200-7299, 8000")
        );
    }

    // ---------------------------------------------------------------
    // Lenient separators — design questions; flip the assertions if the
    // contract should be strict instead
    // ---------------------------------------------------------------

    @Test
    public void parseListTrailingCommaIsIgnored()
    {
        assertEquals(List.of(r(1, 5)), Range.parseList("1-5,"));
    }

    @Test
    public void parseListLeadingCommaIsIgnored()
    {
        assertEquals(List.of(r(1, 5)), Range.parseList(",1-5"));
    }

    @Test
    public void parseListEmptyEntryBetweenCommasIsIgnored()
    {
        assertEquals(
            List.of(r(1, 5), r(10, 15)),
            Range.parseList("1-5,,10-15")
        );
    }

    // ---------------------------------------------------------------
    // Invalid input
    // ---------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void parseListNonNumericThrows()
    {
        Range.parseList("abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListPartiallyNonNumericThrows()
    {
        Range.parseList("1-abc");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListMalformedRangeMissingToThrows()
    {
        Range.parseList("5-");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListThreeNumbersInOneEntryThrows()
    {
        Range.parseList("1-5-10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListNumberTooLargeForIntThrows()
    {
        // > Integer.MAX_VALUE
        Range.parseList("1-9999999999");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListOnlyDashThrows()
    {
        Range.parseList("-");
    }

    // ===============================================================
    // intersects
    // ===============================================================

    // ---------------------------------------------------------------
    // Disjoint (return false)
    // ---------------------------------------------------------------

    @Test
    public void intersectsDisjointRangesReturnFalse()
    {
        assertFalse(r(1, 5).intersects(r(10, 15)));
        assertFalse(r(10, 15).intersects(r(1, 5)));
    }

    @Test
    public void intersectsAdjacentNonOverlappingRangesReturnFalse()
    {
        // (1,5) and (6,10) are adjacent (mergeable) but share no point —
        // intersects must NOT return true here, that's the merger's job
        assertFalse(r(1, 5).intersects(r(6, 10)));
        assertFalse(r(6, 10).intersects(r(1, 5)));
    }

    @Test
    public void intersectsPointJustOutsideOfRangeReturnsFalse()
    {
        assertFalse(r(5, 5).intersects(r(6, 10)));
        assertFalse(r(6, 10).intersects(r(5, 5)));
        assertFalse(r(5, 5).intersects(r(0, 4)));
    }

    @Test
    public void intersectsTwoDifferentPointsReturnFalse()
    {
        assertFalse(r(5, 5).intersects(r(6, 6)));
    }

    // ---------------------------------------------------------------
    // Overlapping (return true)
    // ---------------------------------------------------------------

    @Test
    public void intersectsSharedSingleEndpointReturnsTrue()
    {
        // (1,5) and (5,10) share exactly the point 5 — endpoints are inclusive
        assertTrue(r(1, 5).intersects(r(5, 10)));
        assertTrue(r(5, 10).intersects(r(1, 5)));
    }

    @Test
    public void intersectsPartialOverlapReturnsTrue()
    {
        assertTrue(r(1, 10).intersects(r(5, 15)));
        assertTrue(r(5, 15).intersects(r(1, 10)));
    }

    @Test
    public void intersectsFullyContainedReturnsTrue()
    {
        assertTrue(r(1, 20).intersects(r(5, 10)));
        assertTrue(r(5, 10).intersects(r(1, 20)));
    }

    @Test
    public void intersectsContainmentSharingLowEndpointReturnsTrue()
    {
        assertTrue(r(1, 20).intersects(r(1, 5)));
        assertTrue(r(1, 5).intersects(r(1, 20)));
    }

    @Test
    public void intersectsContainmentSharingHighEndpointReturnsTrue()
    {
        assertTrue(r(1, 20).intersects(r(10, 20)));
        assertTrue(r(10, 20).intersects(r(1, 20)));
    }

    @Test
    public void intersectsIdenticalRangesReturnTrue()
    {
        assertTrue(r(3, 7).intersects(r(3, 7)));
    }

    @Test
    public void intersectsIsReflexive()
    {
        Range range = r(3, 7);
        assertTrue(range.intersects(range));
    }

    // ---------------------------------------------------------------
    // Point ranges
    // ---------------------------------------------------------------

    @Test
    public void intersectsPointInsideRangeReturnsTrue()
    {
        assertTrue(r(5, 5).intersects(r(1, 10)));
        assertTrue(r(1, 10).intersects(r(5, 5)));
    }

    @Test
    public void intersectsTwoIdenticalPointsReturnTrue()
    {
        assertTrue(r(5, 5).intersects(r(5, 5)));
    }

    @Test
    public void intersectsPointEqualToRangeEndpointReturnsTrue()
    {
        assertTrue(r(5, 5).intersects(r(5, 10)));
        assertTrue(r(5, 5).intersects(r(0, 5)));
    }

    // ---------------------------------------------------------------
    // Negative / numeric edges
    // ---------------------------------------------------------------

    @Test
    public void intersectsNegativeRangesOverlap()
    {
        assertTrue(r(-10, -5).intersects(r(-7, 0)));
    }

    @Test
    public void intersectsNegativeRangesDisjoint()
    {
        assertFalse(r(-10, -5).intersects(r(-3, 0)));
    }

    @Test
    public void intersectsRangeAcrossZero()
    {
        assertTrue(r(-3, 3).intersects(r(0, 0)));
    }

    @Test
    public void intersectsAtIntegerMaxValue()
    {
        // guards against overflow if implementation uses (a.to + 1) or similar
        assertTrue(r(0, Integer.MAX_VALUE).intersects(r(Integer.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Test
    public void intersectsAtIntegerMinValue()
    {
        assertTrue(r(Integer.MIN_VALUE, 0).intersects(r(Integer.MIN_VALUE, Integer.MIN_VALUE)));
    }

    @Test
    public void intersectsSpansFullIntegerRange()
    {
        assertTrue(r(Integer.MIN_VALUE, Integer.MAX_VALUE).intersects(r(0, 0)));
        assertTrue(r(0, 0).intersects(r(Integer.MIN_VALUE, Integer.MAX_VALUE)));
    }

    // ---------------------------------------------------------------
    // Symmetry property
    // ---------------------------------------------------------------

    @Test
    public void intersectsIsSymmetric()
    {
        Range[][] pairs = {
            {r(1, 5),  r(10, 15)},  // disjoint
            {r(1, 5),  r(6, 10)},   // adjacent
            {r(1, 5),  r(5, 10)},   // shared endpoint
            {r(1, 10), r(5, 15)},   // partial overlap
            {r(1, 20), r(5, 10)},   // containment
            {r(5, 5),  r(5, 5)},    // identical points
            {r(5, 5),  r(6, 6)},    // adjacent points
            {r(5, 5),  r(1, 10)},   // point inside range
        };

        for (Range[] pair : pairs)
        {
            assertEquals(
                "intersects must be symmetric for " + pair[0] + " / " + pair[1],
                pair[0].intersects(pair[1]),
                pair[1].intersects(pair[0])
            );
        }
    }

    // ===============================================================
    // touches
    // ===============================================================

    // ---------------------------------------------------------------
    // Gap between ranges (return false)
    // ---------------------------------------------------------------

    @Test
    public void touchesDisjointRangesWithGapReturnFalse()
    {
        assertFalse(r(1, 5).touches(r(10, 15)));
        assertFalse(r(10, 15).touches(r(1, 5)));
    }

    @Test
    public void touchesRangesWithSinglePointGapReturnFalse()
    {
        // (1,5) and (7,10) — gap of exactly one number (6)
        assertFalse(r(1, 5).touches(r(7, 10)));
        assertFalse(r(7, 10).touches(r(1, 5)));
    }

    @Test
    public void touchesPointsWithGapReturnFalse()
    {
        assertFalse(r(5, 5).touches(r(7, 7)));
    }

    // ---------------------------------------------------------------
    // Adjacent (return true) — the property that distinguishes
    // touches from intersects
    // ---------------------------------------------------------------

    @Test
    public void touchesAdjacentRangesReturnTrue()
    {
        // (1,5) and (6,10) share no point but are adjacent
        assertTrue(r(1, 5).touches(r(6, 10)));
        assertTrue(r(6, 10).touches(r(1, 5)));
    }

    @Test
    public void touchesAdjacentPointsReturnTrue()
    {
        assertTrue(r(5, 5).touches(r(6, 6)));
        assertTrue(r(6, 6).touches(r(5, 5)));
    }

    @Test
    public void touchesAdjacentNegativeRangesReturnTrue()
    {
        assertTrue(r(-10, -5).touches(r(-4, 0)));
    }

    // ---------------------------------------------------------------
    // Overlap / containment (return true) — same as intersects
    // ---------------------------------------------------------------

    @Test
    public void touchesSharedSingleEndpointReturnsTrue()
    {
        assertTrue(r(1, 5).touches(r(5, 10)));
    }

    @Test
    public void touchesPartialOverlapReturnsTrue()
    {
        assertTrue(r(1, 10).touches(r(5, 15)));
    }

    @Test
    public void touchesFullyContainedReturnsTrue()
    {
        assertTrue(r(1, 20).touches(r(5, 10)));
        assertTrue(r(5, 10).touches(r(1, 20)));
    }

    @Test
    public void touchesIdenticalRangesReturnTrue()
    {
        assertTrue(r(3, 7).touches(r(3, 7)));
    }

    @Test
    public void touchesIsReflexive()
    {
        Range range = r(3, 7);
        assertTrue(range.touches(range));
    }

    // ---------------------------------------------------------------
    // Numeric edges
    // ---------------------------------------------------------------

    @Test
    public void touchesAtIntegerMaxValue()
    {
        // adjacency at MAX_VALUE: (0, MAX_VALUE-1) and (MAX_VALUE, MAX_VALUE)
        assertTrue(r(0, Integer.MAX_VALUE - 1).touches(r(Integer.MAX_VALUE, Integer.MAX_VALUE)));
        // overlap at MAX_VALUE
        assertTrue(r(0, Integer.MAX_VALUE).touches(r(Integer.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Test
    public void touchesAtIntegerMinValue()
    {
        // overlap at MIN_VALUE
        assertTrue(r(Integer.MIN_VALUE, 0).touches(r(Integer.MIN_VALUE, Integer.MIN_VALUE)));
        // adjacency at MIN_VALUE: (MIN_VALUE, MIN_VALUE) and (MIN_VALUE+1, 0)
        assertTrue(r(Integer.MIN_VALUE, Integer.MIN_VALUE).touches(r(Integer.MIN_VALUE + 1, 0)));
    }

    @Test
    public void touchesSpansFullIntegerRange()
    {
        assertTrue(r(Integer.MIN_VALUE, Integer.MAX_VALUE).touches(r(0, 0)));
        assertTrue(r(0, 0).touches(r(Integer.MIN_VALUE, Integer.MAX_VALUE)));
    }

    // ---------------------------------------------------------------
    // Symmetry
    // ---------------------------------------------------------------

    @Test
    public void touchesIsSymmetric()
    {
        Range[][] pairs = {
            {r(1, 5),  r(10, 15)},  // disjoint
            {r(1, 5),  r(6, 10)},   // adjacent
            {r(1, 5),  r(5, 10)},   // shared endpoint
            {r(1, 10), r(5, 15)},   // partial overlap
            {r(1, 20), r(5, 10)},   // containment
            {r(5, 5),  r(6, 6)},    // adjacent points
            {r(5, 5),  r(7, 7)},    // points with gap
        };

        for (Range[] pair : pairs)
        {
            assertEquals(
                "touches must be symmetric for " + pair[0] + " / " + pair[1],
                pair[0].touches(pair[1]),
                pair[1].touches(pair[0])
            );
        }
    }

    // ===============================================================
    // Constructor — canonical form
    // ===============================================================

    @Test
    public void constructorSwapsWhenFromGreaterThanTo()
    {
        Range range = new Range(10, 5);
        assertEquals(5, range.from());
        assertEquals(10, range.to());
    }

    @Test
    public void constructorSwapsAtIntegerBoundaries()
    {
        Range range = new Range(Integer.MAX_VALUE, Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, range.from());
        assertEquals(Integer.MAX_VALUE, range.to());
    }

    @Test
    public void constructorPointRangeNotMutated()
    {
        Range range = new Range(7, 7);
        assertEquals(7, range.from());
        assertEquals(7, range.to());
    }

    // ===============================================================
    // compareTo
    // ===============================================================

    @Test
    public void compareToOrdersByFromAscending()
    {
        assertTrue(r(1, 100).compareTo(r(2, 3)) < 0);
        assertTrue(r(2, 3).compareTo(r(1, 100)) > 0);
    }

    @Test
    public void compareToBreaksTiesByToAscending()
    {
        // identical from, smaller to comes first
        assertTrue(r(5, 10).compareTo(r(5, 20)) < 0);
        assertTrue(r(5, 20).compareTo(r(5, 10)) > 0);
    }

    @Test
    public void compareToReturnsZeroForEqualRanges()
    {
        assertEquals(0, r(5, 10).compareTo(r(5, 10)));
    }

    @Test
    public void compareToHandlesNegatives()
    {
        assertTrue(r(-10, -5).compareTo(r(-1, 0)) < 0);
        assertTrue(r(-1, 0).compareTo(r(-10, -5)) > 0);
    }

    @Test
    public void compareToAtIntegerBoundariesDoesNotOverflow()
    {
        // a naive (a.from - b.from) implementation would overflow here
        assertTrue(r(Integer.MIN_VALUE, 0).compareTo(r(Integer.MAX_VALUE, Integer.MAX_VALUE)) < 0);
        assertTrue(r(Integer.MAX_VALUE, Integer.MAX_VALUE).compareTo(r(Integer.MIN_VALUE, 0)) > 0);
    }

    // ===============================================================
    // createMerged
    // ===============================================================

    @Test
    public void createMergedOverlappingRangesYieldsUnion()
    {
        assertEquals(r(1, 15), r(1, 10).createMerged(r(5, 15)));
    }

    @Test
    public void createMergedAdjacentRangesYieldsUnion()
    {
        assertEquals(r(1, 10), r(1, 5).createMerged(r(6, 10)));
    }

    @Test
    public void createMergedDisjointRangesYieldsConvexHullNotTrueUnion()
    {
        // createMerged returns the convex hull (min..max), not a true set union.
        // Callers (e.g. RangeMerger) are expected to invoke this only when ranges touch.
        assertEquals(r(1, 20), r(1, 5).createMerged(r(15, 20)));
    }

    @Test
    public void createMergedIsCommutative()
    {
        assertEquals(
            r(1, 10).createMerged(r(5, 15)),
            r(5, 15).createMerged(r(1, 10))
        );
    }

    @Test
    public void createMergedIsIdempotent()
    {
        Range range = r(3, 7);
        assertEquals(range, range.createMerged(range));
    }

    @Test
    public void createMergedFullyContainedReturnsOuter()
    {
        assertEquals(r(1, 20), r(1, 20).createMerged(r(5, 10)));
        assertEquals(r(1, 20), r(5, 10).createMerged(r(1, 20)));
    }

    @Test
    public void createMergedAtIntegerBoundaries()
    {
        assertEquals(
            r(Integer.MIN_VALUE, Integer.MAX_VALUE),
            r(Integer.MIN_VALUE, 0).createMerged(r(0, Integer.MAX_VALUE))
        );
    }

    // ===============================================================
    // parseList — additional corner cases
    // ===============================================================

    @Test
    public void parseListNegativeFromPositiveTo()
    {
        // "-5-7" - first dash starts a negative number, second separates from .. to
        assertEquals(List.of(r(-5, 7)), Range.parseList("-5-7"));
    }

    @Test
    public void parseListPositiveFromNegativeToSwaps()
    {
        // "5--7" - 5 to -7, then constructor swaps to (-7, 5)
        assertEquals(List.of(r(-7, 5)), Range.parseList("5--7"));
    }

    @Test
    public void parseListIntegerMaxValue()
    {
        assertEquals(List.of(r(Integer.MAX_VALUE, Integer.MAX_VALUE)), Range.parseList("2147483647"));
    }

    @Test
    public void parseListIntegerMinValue()
    {
        assertEquals(List.of(r(Integer.MIN_VALUE, Integer.MIN_VALUE)), Range.parseList("-2147483648"));
    }

    @Test
    public void parseListIntegerMinToMax()
    {
        assertEquals(
            List.of(r(Integer.MIN_VALUE, Integer.MAX_VALUE)),
            Range.parseList("-2147483648-2147483647")
        );
    }

    @Test
    public void parseListMultipleConsecutiveCommasProduceEmptyList()
    {
        assertEquals(List.of(), Range.parseList(",,,"));
    }

    @Test
    public void parseListWhitespaceOnlyEntriesAreIgnored()
    {
        assertEquals(List.of(r(1, 5)), Range.parseList(" , , 1-5 ,  "));
    }

    @Test
    public void parseListLeadingZerosAccepted()
    {
        // Integer.parseInt handles leading zeros as decimal (not octal)
        assertEquals(List.of(r(7, 10)), Range.parseList("007-010"));
    }

    @Test
    public void parseListReversedNegativeRangeSwapped()
    {
        // "-5--10" → (-5, -10) → swapped to (-10, -5)
        assertEquals(List.of(r(-10, -5)), Range.parseList("-5--10"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListDoubleNegativeAtStartThrows()
    {
        // "--5" → fromStr="-", parseInt("-") throws
        Range.parseList("--5");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseListWhitespaceInsideNumberThrows()
    {
        Range.parseList("1 2-5");
    }
}
