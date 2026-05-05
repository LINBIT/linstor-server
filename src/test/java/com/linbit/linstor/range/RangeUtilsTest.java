package com.linbit.linstor.range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class RangeUtilsTest
{
    private static Range r(int from, int to)
    {
        return new Range(from, to);
    }

    private static List<Range> listOf(Range... ranges)
    {
        return new ArrayList<>(Arrays.asList(ranges));
    }

    // ---------------------------------------------------------------
    // Trivial / boundary inputs
    // ---------------------------------------------------------------

    @Test
    public void nullInputReturnsEmptyList()
    {
        List<Range> result = RangeUtils.merge(null);

        assertTrue("expected empty list for null input", result.isEmpty());
    }

    @Test
    public void emptyInputReturnsEmptyList()
    {
        List<Range> result = RangeUtils.merge(listOf());

        assertTrue("expected empty list for empty input", result.isEmpty());
    }

    @Test
    public void singleRangeReturnsSameRange()
    {
        List<Range> result = RangeUtils.merge(listOf(r(5, 10)));

        assertEquals(List.of(r(5, 10)), result);
    }

    @Test
    public void singlePointRangeIsPreserved()
    {
        List<Range> result = RangeUtils.merge(listOf(r(5, 5)));

        assertEquals(List.of(r(5, 5)), result);
    }

    // ---------------------------------------------------------------
    // Two-range relative positions
    // ---------------------------------------------------------------

    @Test
    public void disjointRangesWithGapAreNotMerged()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(10, 15)));

        assertEquals(List.of(r(1, 5), r(10, 15)), result);
    }

    @Test
    public void adjacentRangesAreMerged()
    {
        // javadoc example
        List<Range> result = RangeUtils.merge(listOf(r(10, 15), r(16, 20)));

        assertEquals(List.of(r(10, 20)), result);
    }

    @Test
    public void partiallyOverlappingRangesAreMerged()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 7), r(5, 10)));

        assertEquals(List.of(r(1, 10)), result);
        result = RangeUtils.merge(listOf(r(5, 10), r(1, 7)));

        assertEquals(List.of(r(1, 10)), result);
    }

    @Test
    public void rangesSharingOneEndpointAreMerged()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(5, 10)));

        assertEquals(List.of(r(1, 10)), result);
    }

    @Test
    public void identicalRangesAreCollapsed()
    {
        List<Range> result = RangeUtils.merge(listOf(r(3, 7), r(3, 7)));

        assertEquals(List.of(r(3, 7)), result);
    }

    @Test
    public void fullyContainedRangeIsAbsorbed()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 20), r(5, 10)));

        assertEquals(List.of(r(1, 20)), result);
    }

    @Test
    public void containmentWithSharedLowEndpoint()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 20), r(1, 5)));

        assertEquals(List.of(r(1, 20)), result);
    }

    @Test
    public void containmentWithSharedHighEndpoint()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 20), r(10, 20)));

        assertEquals(List.of(r(1, 20)), result);
    }

    // ---------------------------------------------------------------
    // Order / sorting
    // ---------------------------------------------------------------

    @Test
    public void unsortedInputMergesCorrectly()
    {
        // (1,5) + (6,9) adjacent -> (1,9); (1,9) + (10,15) adjacent -> (1,15)
        List<Range> result = RangeUtils.merge(listOf(r(10, 15), r(1, 5), r(6, 9)));

        assertEquals(List.of(r(1, 15)), result);
    }

    @Test
    public void reverseSortedNonMergeableRangesAreReturnedAscending()
    {
        List<Range> result = RangeUtils.merge(listOf(r(20, 25), r(10, 15)));

        assertEquals(List.of(r(10, 15), r(20, 25)), result);
    }

    @Test
    public void outputIsSortedByFromAscending()
    {
        List<Range> result = RangeUtils.merge(listOf(r(50, 60), r(1, 5), r(20, 25), r(10, 12)));

        for (int idx = 1; idx < result.size(); idx++)
        {
            assertTrue(
                "result not sorted ascending by 'from' at index " + idx + ": " + result,
                result.get(idx - 1).from() <= result.get(idx).from()
            );
        }
        assertEquals(List.of(r(1, 5), r(10, 12), r(20, 25), r(50, 60)), result);
    }

    // ---------------------------------------------------------------
    // Many ranges
    // ---------------------------------------------------------------

    @Test
    public void chainOfAdjacentRangesCollapsesToOne()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 2), r(3, 4), r(5, 6), r(7, 8)));

        assertEquals(List.of(r(1, 8)), result);
    }

    @Test
    public void mixedMergeableAndNonMergeableGroups()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(6, 10), r(20, 25), r(26, 30)));

        assertEquals(List.of(r(1, 10), r(20, 30)), result);
    }

    @Test
    public void cascadingOverlapsCollapseToOne()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 10), r(5, 15), r(12, 20)));

        assertEquals(List.of(r(1, 20)), result);
    }

    @Test
    public void manyDuplicatesCollapse()
    {
        List<Range> result = RangeUtils.merge(listOf(r(3, 7), r(3, 7), r(3, 7), r(3, 7)));

        assertEquals(List.of(r(3, 7)), result);
    }

    @Test
    public void manyPointRangesPartiallyAdjacent()
    {
        // (5,5)+(6,6) adjacent -> (5,6); (8,8) standalone
        List<Range> result = RangeUtils.merge(listOf(r(5, 5), r(6, 6), r(8, 8)));

        assertEquals(List.of(r(5, 6), r(8, 8)), result);
    }

    // ---------------------------------------------------------------
    // Negative / numeric edges
    // ---------------------------------------------------------------

    @Test
    public void negativeRangesMerge()
    {
        // -5 and -4 are adjacent
        List<Range> result = RangeUtils.merge(listOf(r(-10, -5), r(-4, 0)));

        assertEquals(List.of(r(-10, 0)), result);
    }

    @Test
    public void rangeCrossingZeroIsPreserved()
    {
        List<Range> result = RangeUtils.merge(listOf(r(-3, 3)));

        assertEquals(List.of(r(-3, 3)), result);
    }

    @Test
    public void adjacencyAtIntegerMaxValueDoesNotOverflow()
    {
        // guards against a naive `current.to + 1 == next.from` check that would overflow
        List<Range> result = RangeUtils.merge(listOf(r(0, Integer.MAX_VALUE), r(Integer.MAX_VALUE, Integer.MAX_VALUE)));

        assertEquals(List.of(r(0, Integer.MAX_VALUE)), result);
    }

    @Test
    public void adjacencyAtIntegerMinValueDoesNotOverflow()
    {
        // symmetric guard against `next.from - 1` underflow
        List<Range> result = RangeUtils.merge(listOf(r(Integer.MIN_VALUE, Integer.MIN_VALUE), r(Integer.MIN_VALUE, 0)));

        assertEquals(List.of(r(Integer.MIN_VALUE, 0)), result);
    }

    @Test
    public void integerMinMaxValue()
    {
        // symmetric guard against `next.from - 1` underflow
        List<Range> result = RangeUtils.merge(listOf(r(Integer.MIN_VALUE, 9001), r(0, Integer.MAX_VALUE)));

        assertEquals(List.of(r(Integer.MIN_VALUE, Integer.MAX_VALUE)), result);
    }

    // ---------------------------------------------------------------
    // Non-mutation / aliasing contract
    // ---------------------------------------------------------------

    @Test
    public void inputListIsNotMutated()
    {
        List<Range> input = listOf(r(10, 15), r(1, 5), r(6, 9));
        List<Range> copy = new ArrayList<>(input);

        RangeUtils.merge(input);

        assertEquals("merge() must not mutate its input list", copy, input);
    }

    @Test
    public void returnedListIsIndependentOfInput()
    {
        List<Range> input = listOf(r(1, 5), r(20, 25));
        List<Range> copy = new ArrayList<>(input);

        List<Range> result = RangeUtils.merge(input);
        assertNotSame("returned list must not be the same instance as the input", input, result);

        // mutating the result must not affect the input
        result.clear();
        assertEquals("mutating the returned list must not affect the input", copy, input);
    }

    // ---------------------------------------------------------------
    // Design questions — decide the contract, then assert it
    // ---------------------------------------------------------------

    @Test
    public void invalidRangeWhereFromGreaterThanTo()
    {
        List<Range> input = listOf(r(10, 5));
        List<Range> result = RangeUtils.merge(input);

        assertEquals("range should be sorted", listOf(r(5, 10)), input);
        assertEquals("range should be sorted", listOf(r(5, 10)), result);
    }

    @Test
    public void nullElementInsideListIsHandled()
    {
        List<Range> input = new ArrayList<>(Arrays.asList(r(1, 5), null, r(10, 15)));
        List<Range> result = RangeUtils.merge(input);
        assertEquals("null element should have been skipped", listOf(r(1, 5), r(10, 15)), result);
    }

    @Test
    public void allNullElementsInListYieldEmptyResult()
    {
        List<Range> input = new ArrayList<>(Arrays.asList(null, null, null));
        List<Range> result = RangeUtils.merge(input);
        assertTrue("all-null input should produce empty list", result.isEmpty());
    }

    // ---------------------------------------------------------------
    // Regression — rangeToMerge sorts before the first existing range
    // AND the merged result extends into subsequent ranges.
    // simpleMergeFirst must not stop at the first element.
    // ---------------------------------------------------------------

    @Test
    public void rangeBeforeFirstExtendingIntoSecondMergesAll()
    {
        // (5,22) sorts before (10,15). Merging them yields (5,22), which
        // overlaps (20,25) → final result must collapse to (5,25).
        List<Range> result = RangeUtils.merge(listOf(r(10, 15), r(20, 25), r(5, 22)));
        assertEquals(List.of(r(5, 25)), result);
    }

    @Test
    public void rangeBeforeFirstExtendingExactlyToTouchSecondMergesAll()
    {
        // (5,19) merged with (10,15) -> (5,19); 19+1 == 20 so it touches (20,25).
        List<Range> result = RangeUtils.merge(listOf(r(10, 15), r(20, 25), r(5, 19)));
        assertEquals(List.of(r(5, 25)), result);
    }

    @Test
    public void rangeBeforeFirstAbsorbsAllExistingRanges()
    {
        // (1,100) covers everything: (5,10), (15,20), (25,30) all collapse into it.
        List<Range> result = RangeUtils.merge(listOf(r(5, 10), r(15, 20), r(25, 30), r(1, 100)));
        assertEquals(List.of(r(1, 100)), result);
    }

    @Test
    public void rangeBeforeFirstAbsorbsMiddleButLeavesLastDisjoint()
    {
        // (1,22) absorbs (5,10) and (15,20); (50,60) is far enough away to remain.
        List<Range> result = RangeUtils.merge(listOf(r(5, 10), r(15, 20), r(50, 60), r(1, 22)));
        assertEquals(List.of(r(1, 22), r(50, 60)), result);
    }

    @Test
    public void rangeBeforeFirstSharingFromAndExtendingIntoSecondMergesAll()
    {
        // same `from` as first (5,10) but reaches into (20,25) → simpleMerge dispatches
        // via rangeToMerge.compareTo(first) < 0 because to=22 < 10? no, 22>10 means greater.
        // Actually compareTo here returns >0 (same from, larger to). So this falls to mergeImpl
        // and is a control case: mergeImpl already handles this correctly.
        List<Range> result = RangeUtils.merge(listOf(r(5, 10), r(20, 25), r(5, 22)));
        assertEquals(List.of(r(5, 25)), result);
    }

    // ---------------------------------------------------------------
    // Additional coverage — rangeToMerge straddles middle ranges
    // ---------------------------------------------------------------

    @Test
    public void rangeStraddlingTwoMiddleRangesAbsorbsBoth()
    {
        // (12,22) overlaps (10,15) and (20,25) but not (1,5) or (30,35).
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(10, 15), r(20, 25), r(30, 35), r(12, 22)));
        assertEquals(List.of(r(1, 5), r(10, 25), r(30, 35)), result);
    }

    @Test
    public void rangeAbsorbsThreeContiguousMiddleRanges()
    {
        List<Range> result = RangeUtils.merge(
            listOf(r(1, 5), r(10, 15), r(20, 25), r(30, 35), r(50, 60), r(12, 33))
        );
        assertEquals(List.of(r(1, 5), r(10, 35), r(50, 60)), result);
    }

    @Test
    public void rangeFillingGapBetweenTwoRangesExactlyAdjacentBothSides()
    {
        // (6,9) fills the gap between (1,5) and (10,15) and is adjacent to both.
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(10, 15), r(6, 9)));
        assertEquals(List.of(r(1, 15)), result);
    }

    @Test
    public void rangeEqualToMiddleExistingRangeIsCollapsed()
    {
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(10, 15), r(20, 25), r(10, 15)));
        assertEquals(List.of(r(1, 5), r(10, 15), r(20, 25)), result);
    }

    @Test
    public void rangeStrictlyInsideExistingRangeIsAbsorbedNotInserted()
    {
        // (12,13) is strictly inside (10,15); must not introduce a duplicate or gap.
        List<Range> result = RangeUtils.merge(listOf(r(1, 5), r(10, 15), r(20, 25), r(12, 13)));
        assertEquals(List.of(r(1, 5), r(10, 15), r(20, 25)), result);
    }

    // ---------------------------------------------------------------
    // Two-arg merge(orderedRanges, range) overload — direct calls.
    // The current tests all go through merge(List), so the overload is
    // never exercised in isolation.
    // ---------------------------------------------------------------

    @Test
    public void directMergeIntoEmptyOrderedList()
    {
        List<Range> result = RangeUtils.merge(List.of(), r(5, 10));
        assertEquals(List.of(r(5, 10)), result);
    }

    @Test
    public void directMergeBeforeFirstWithoutTouching()
    {
        List<Range> result = RangeUtils.merge(List.of(r(10, 15), r(20, 25)), r(1, 5));
        assertEquals(List.of(r(1, 5), r(10, 15), r(20, 25)), result);
    }

    @Test
    public void directMergeAfterLastWithoutTouching()
    {
        List<Range> result = RangeUtils.merge(List.of(r(1, 5), r(10, 15)), r(30, 35));
        assertEquals(List.of(r(1, 5), r(10, 15), r(30, 35)), result);
    }

    @Test
    public void directMergeIntoMiddleGap()
    {
        // exercises mergeImpl's "isInsertable" branch directly
        List<Range> result = RangeUtils.merge(List.of(r(1, 5), r(20, 25), r(40, 45)), r(10, 15));
        assertEquals(List.of(r(1, 5), r(10, 15), r(20, 25), r(40, 45)), result);
    }

    @Test
    public void directMergeWithTouchingFirstStaysCorrectAcrossList()
    {
        // direct exercise of the simpleMergeFirst regression case.
        List<Range> result = RangeUtils.merge(List.of(r(10, 15), r(20, 25)), r(5, 22));
        assertEquals(List.of(r(5, 25)), result);
    }

    // ---------------------------------------------------------------
    // subtract — trivial / boundary inputs
    // ---------------------------------------------------------------

    @Test
    public void subtractEmptyFromEmptyReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(listOf(), listOf());

        assertTrue("expected empty list", result.isEmpty());
    }

    @Test
    public void subtractEmptyFromListReturnsOriginal()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100), r(200, 300)), listOf());

        assertEquals(List.of(r(10, 100), r(200, 300)), result);
    }

    @Test
    public void subtractFromEmptyReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(listOf(), listOf(r(10, 100)));

        assertTrue("expected empty list", result.isEmpty());
    }

    @Test
    public void subtractDocExample()
    {
        // [(10,100), (200,300)] - [(80,90), (290,400)] = [(10,79), (91,100), (200,289)]
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 100), r(200, 300)),
            listOf(r(80, 90), r(290, 400))
        );

        assertEquals(List.of(r(10, 79), r(91, 100), r(200, 289)), result);
    }

    // ---------------------------------------------------------------
    // subtract — single source vs single subtractor positions
    // ---------------------------------------------------------------

    @Test
    public void subtractDisjointBeforeSourceLeavesSourceUnchanged()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(1, 5)));

        assertEquals(List.of(r(10, 100)), result);
    }

    @Test
    public void subtractDisjointAfterSourceLeavesSourceUnchanged()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(200, 300)));

        assertEquals(List.of(r(10, 100)), result);
    }

    @Test
    public void subtractAdjacentBeforeSourceLeavesSourceUnchanged()
    {
        // (1,9) is adjacent to (10,100) but does not intersect — source must be unchanged
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(1, 9)));

        assertEquals(List.of(r(10, 100)), result);
    }

    @Test
    public void subtractAdjacentAfterSourceLeavesSourceUnchanged()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(101, 200)));

        assertEquals(List.of(r(10, 100)), result);
    }

    @Test
    public void subtractStrictlyInsideSourceSplitsRange()
    {
        // (50,60) inside (10,100) → (10,49), (61,100)
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(50, 60)));

        assertEquals(List.of(r(10, 49), r(61, 100)), result);
    }

    @Test
    public void subtractTrimsLeftOfSource()
    {
        // (5,50) overlaps left of (10,100) → (51,100)
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(5, 50)));

        assertEquals(List.of(r(51, 100)), result);
    }

    @Test
    public void subtractTrimsRightOfSource()
    {
        // (50,150) overlaps right of (10,100) → (10,49)
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(50, 150)));

        assertEquals(List.of(r(10, 49)), result);
    }

    @Test
    public void subtractCoveringSourceReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(5, 200)));

        assertTrue("source fully covered should yield empty list", result.isEmpty());
    }

    @Test
    public void subtractIdenticalRangeReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(10, 100)));

        assertTrue("subtracting identical range should yield empty list", result.isEmpty());
    }

    @Test
    public void subtractAlignedAtSourceFromTrimsLeft()
    {
        // (10,50) shares `from` with (10,100) → (51,100)
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(10, 50)));

        assertEquals(List.of(r(51, 100)), result);
    }

    @Test
    public void subtractAlignedAtSourceToTrimsRight()
    {
        // (50,100) shares `to` with (10,100) → (10,49)
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(50, 100)));

        assertEquals(List.of(r(10, 49)), result);
    }

    // ---------------------------------------------------------------
    // subtract — point ranges
    // ---------------------------------------------------------------

    @Test
    public void subtractPointInsideSourceSplits()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(50, 50)));

        assertEquals(List.of(r(10, 49), r(51, 100)), result);
    }

    @Test
    public void subtractPointAtSourceFromTrimsByOne()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(10, 10)));

        assertEquals(List.of(r(11, 100)), result);
    }

    @Test
    public void subtractPointAtSourceToTrimsByOne()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(10, 100)), listOf(r(100, 100)));

        assertEquals(List.of(r(10, 99)), result);
    }

    @Test
    public void subtractPointFromPointReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(listOf(r(50, 50)), listOf(r(50, 50)));

        assertTrue("point - same point should be empty", result.isEmpty());
    }

    // ---------------------------------------------------------------
    // subtract — multi-range scenarios
    // ---------------------------------------------------------------

    @Test
    public void subtractMultipleHolesFromSingleSource()
    {
        // (10,100) - [(20,30),(50,60),(80,90)] → (10,19),(31,49),(61,79),(91,100)
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 100)),
            listOf(r(20, 30), r(50, 60), r(80, 90))
        );

        assertEquals(List.of(r(10, 19), r(31, 49), r(61, 79), r(91, 100)), result);
    }

    @Test
    public void subtractSingleRangeSpanningGapBetweenSources()
    {
        // [(10,50),(60,100)] - [(40,70)] → [(10,39),(71,100)]
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 50), r(60, 100)),
            listOf(r(40, 70))
        );

        assertEquals(List.of(r(10, 39), r(71, 100)), result);
    }

    @Test
    public void subtractRemovesEntireMiddleSourceRange()
    {
        // [(10,20),(50,60),(90,100)] - [(50,60)] → [(10,20),(90,100)]
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 20), r(50, 60), r(90, 100)),
            listOf(r(50, 60))
        );

        assertEquals(List.of(r(10, 20), r(90, 100)), result);
    }

    @Test
    public void subtractCoveringAllSourcesReturnsEmpty()
    {
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 20), r(50, 60), r(90, 100)),
            listOf(r(0, 200))
        );

        assertTrue("all sources covered should yield empty list", result.isEmpty());
    }

    @Test
    public void subtractRangeStraddlingMultipleSourcesTrimsAndDrops()
    {
        // [(10,20),(30,40),(50,60)] - [(15,55)]
        // (10,20) → (10,14); (30,40) fully covered → dropped; (50,60) → (56,60)
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 20), r(30, 40), r(50, 60)),
            listOf(r(15, 55))
        );

        assertEquals(List.of(r(10, 14), r(56, 60)), result);
    }

    @Test
    public void subtractWithOverlappingSubtractors()
    {
        // (10,100) - [(20,40),(30,50)] → (10,19),(51,100)
        // overlapping subtractors should be handled as if their union were subtracted
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 100)),
            listOf(r(20, 40), r(30, 50))
        );

        assertEquals(List.of(r(10, 19), r(51, 100)), result);
    }

    @Test
    public void subtractWithAdjacentSubtractorsCollapsesTheirHole()
    {
        // (10,100) - [(20,30),(31,40)] → (10,19),(41,100)
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 100)),
            listOf(r(20, 30), r(31, 40))
        );

        assertEquals(List.of(r(10, 19), r(41, 100)), result);
    }

    @Test
    public void subtractDuplicateSubtractorsMatchSingleSubtractor()
    {
        List<Range> result = RangeUtils.subtract(
            listOf(r(10, 100)),
            listOf(r(50, 60), r(50, 60), r(50, 60))
        );

        assertEquals(List.of(r(10, 49), r(61, 100)), result);
    }

    // ---------------------------------------------------------------
    // subtract — negative / numeric edges
    // ---------------------------------------------------------------

    @Test
    public void subtractWithNegativeRanges()
    {
        // (-100,100) - (-50,50) → (-100,-51), (51,100)
        List<Range> result = RangeUtils.subtract(listOf(r(-100, 100)), listOf(r(-50, 50)));

        assertEquals(List.of(r(-100, -51), r(51, 100)), result);
    }

    @Test
    public void subtractAtIntegerMinValueDoesNotUnderflow()
    {
        // trimming at MIN_VALUE: a naive `from-1` would underflow when the subtractor's
        // `to` equals Integer.MIN_VALUE. Source must remain unchanged.
        List<Range> result = RangeUtils.subtract(
            listOf(r(Integer.MIN_VALUE, 0)),
            listOf(r(Integer.MIN_VALUE, Integer.MIN_VALUE))
        );

        assertEquals(List.of(r(Integer.MIN_VALUE + 1, 0)), result);
    }

    @Test
    public void subtractAtIntegerMaxValueDoesNotOverflow()
    {
        // symmetric guard: a naive `to+1` would overflow when subtractor's `from` is MAX.
        List<Range> result = RangeUtils.subtract(
            listOf(r(0, Integer.MAX_VALUE)),
            listOf(r(Integer.MAX_VALUE, Integer.MAX_VALUE))
        );

        assertEquals(List.of(r(0, Integer.MAX_VALUE - 1)), result);
    }

    // ---------------------------------------------------------------
    // subtract — non-mutation / aliasing contract
    // ---------------------------------------------------------------

    @Test
    public void subtractDoesNotMutateInputs()
    {
        List<Range> source = listOf(r(10, 100), r(200, 300));
        List<Range> sub = listOf(r(80, 90), r(290, 400));
        List<Range> sourceCopy = new ArrayList<>(source);
        List<Range> subCopy = new ArrayList<>(sub);

        RangeUtils.subtract(source, sub);

        assertEquals("subtract() must not mutate its source list", sourceCopy, source);
        assertEquals("subtract() must not mutate its subtractor list", subCopy, sub);
    }

    @Test
    public void subtractReturnedListIsIndependentOfInputs()
    {
        List<Range> source = listOf(r(10, 100));
        List<Range> sub = listOf(r(50, 50));
        List<Range> sourceCopy = new ArrayList<>(source);

        List<Range> result = RangeUtils.subtract(source, sub);
        assertNotSame("returned list must not be the same instance as the source", source, result);

        result.clear();
        assertEquals("mutating the returned list must not affect the source", sourceCopy, source);
    }

    // ---------------------------------------------------------------
    // render — basic formatting
    // ---------------------------------------------------------------

    @Test
    public void renderSingleMultiPointRange()
    {
        assertEquals("10-20", RangeUtils.render(listOf(r(10, 20))));
    }

    @Test
    public void renderSinglePointRangeIsRenderedWithoutDash()
    {
        // from == to must be rendered as a single number, not "5-5"
        assertEquals("5", RangeUtils.render(listOf(r(5, 5))));
    }

    @Test
    public void renderMultipleMultiPointRangesAreCommaSeparated()
    {
        assertEquals("1-5,10-20", RangeUtils.render(listOf(r(1, 5), r(10, 20))));
    }

    @Test
    public void renderMultiplePointRangesAreCommaSeparated()
    {
        assertEquals("1,5,9", RangeUtils.render(listOf(r(1, 1), r(5, 5), r(9, 9))));
    }

    @Test
    public void renderMixOfPointAndMultiPointRanges()
    {
        // mirrors the use case in DrbdAdjustBlockedPortHandler: blocked TCP ports
        // are typically a mix of singletons and contiguous bands.
        assertEquals("5,10-20,30", RangeUtils.render(listOf(r(5, 5), r(10, 20), r(30, 30))));
    }

    @Test
    public void renderDoesNotAddTrailingComma()
    {
        String result = RangeUtils.render(listOf(r(1, 5), r(10, 20)));
        assertEquals("must not end with a separator", false, result.endsWith(","));
    }

    // ---------------------------------------------------------------
    // render — empty input
    // ---------------------------------------------------------------

    @Test
    public void renderEmptyListReturnsEmptyString()
    {
        assertEquals("", RangeUtils.render(listOf()));
        assertEquals("", RangeUtils.render(null));
    }

    // ---------------------------------------------------------------
    // render — negative bounds
    // ---------------------------------------------------------------

    @Test
    public void renderNegativeMultiPointRange()
    {
        // verifies the format chosen for negative bounds: "from-to" with literal '-' separator
        assertEquals("-5--1", RangeUtils.render(listOf(r(-5, -1))));
    }

    @Test
    public void renderNegativeSinglePointRange()
    {
        assertEquals("-5", RangeUtils.render(listOf(r(-5, -5))));
    }

    @Test
    public void renderRangeCrossingZero()
    {
        assertEquals("-3-3", RangeUtils.render(listOf(r(-3, 3))));
    }

    // ---------------------------------------------------------------
    // render — numeric edges
    // ---------------------------------------------------------------

    @Test
    public void renderRangeAtIntegerMaxValue()
    {
        assertEquals(
            String.valueOf(Integer.MAX_VALUE),
            RangeUtils.render(listOf(r(Integer.MAX_VALUE, Integer.MAX_VALUE)))
        );
    }

    @Test
    public void renderRangeAtIntegerMinValue()
    {
        assertEquals(
            String.valueOf(Integer.MIN_VALUE),
            RangeUtils.render(listOf(r(Integer.MIN_VALUE, Integer.MIN_VALUE)))
        );
    }

    @Test
    public void renderFullIntegerRange()
    {
        assertEquals(
            Integer.MIN_VALUE + "-" + Integer.MAX_VALUE,
            RangeUtils.render(listOf(r(Integer.MIN_VALUE, Integer.MAX_VALUE)))
        );
    }

    // ---------------------------------------------------------------
    // render — non-mutation / round-trip
    // ---------------------------------------------------------------

    @Test
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    public void renderDoesNotMutateInput()
    {
        List<Range> input = listOf(r(1, 5), r(10, 20), r(30, 30));
        List<Range> copy = new ArrayList<>(input);

        RangeUtils.render(input);

        assertEquals("render() must not mutate its input list", copy, input);
    }

    @Test
    public void renderRoundTripsWithParseList()
    {
        // render is the inverse of Range.parseList. Picking a representative mix
        // (point, multi-point, negative, crossing zero) catches accidental format drift.
        List<Range> original = listOf(r(-10, -5), r(-3, 3), r(7, 7), r(20, 100));
        String rendered = RangeUtils.render(original);

        assertEquals(original, Range.parseList(rendered));
    }

    // ---------------------------------------------------------------
    // render — full circle
    // ---------------------------------------------------------------

    @Test
    public void renderSimplifiedVersionOfParsed()
    {
        assertEquals("10-20", RangeUtils.render(Range.parseList("12-18,19-20,10,10-15")));
        assertEquals("1-5", RangeUtils.render(Range.parseList("1-3,3-5")));
    }
}
