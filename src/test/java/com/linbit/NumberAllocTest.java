package com.linbit;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Number allocation algorithm tests
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NumberAllocTest
{
    public static final ValueTest[] TEST_START_INDEX =
    {
        // Test no occupied elements
        new ValueTest(new int[] {}, 0, 0),
        new ValueTest(new int[] {}, -7, 0),
        new ValueTest(new int[] {}, 11, 0),

        // Test start index in the middle
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 11, 7),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 10, 7),

        // Test start index 0
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, -3, 0),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, -9, 0),

        // Test start index before last element
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 21}, 20, 14),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 21}, 21, 14),

        // Test start index behind last element
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 21}, 22, 15),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 21}, 30, 15)
    };

    public static final ValueTest[] TEST_END_INDEX =
    {
        // Test no occupied elements
        new ValueTest(new int[] {}, 0, 0),
        new ValueTest(new int[] {}, -7, 0),
        new ValueTest(new int[] {}, 11, 0),

        // Test end index in the middle
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 17, 12),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 6, 5),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 4, 5),

        // Test end index 0
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, -4, 0),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, -5, 0),

        // Test end index after first element
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, -3, 1),

        // Test end index behind last element
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 20, 15),
        new ValueTest(new int[] {-3, -2, 0, 3, 4, 7, 9, 11, 12, 13, 15, 17, 18, 19, 20}, 30, 15)
    };

    public static final RangeTest[] TEST_FREE_NUMBER =
    {
        // Test free number at index 0
        new RangeTest(new int[] {31, 32, 33, 34, 35, 37, 38, 39}, 30, 50, 30),
        new RangeTest(new int[] {31, 32, 33, 34, 35, 37, 38, 39}, 20, 40, 20),
        new RangeTest(new int[] {31, 32, 33, 34, 35, 37, 38, 39}, 0, 50, 0),
        new RangeTest(new int[] {31, 32, 33, 34, 35, 37, 38, 39}, -5, 40, -5),

        // Test free number in the middle
        new RangeTest(new int[] {30, 31, 32, 33, 35, 37, 38, 39}, 30, 50, 34),
        new RangeTest(new int[] {30, 31, 32, 33, 34, 37, 38, 39}, 30, 40, 35),
        new RangeTest(new int[] {30, 31, 32, 33, 34, 35, 38, 39}, 30, 50, 36),

        // Test free number behind the last element
        new RangeTest(new int[] {30, 31, 32, 33, 34, 35, 36, 37}, 30, 40, 38),

        // Test free number after the first element
        new RangeTest(new int[] {30, 32, 34, 35, 36, 37, 38, 39}, 30, 50, 31),
        new RangeTest(new int[] {30, 33, 34, 35, 36, 37, 38, 39}, 30, 40, 31),

        // Test free number before the last element
        new RangeTest(new int[] {30, 31, 32, 33, 34, 35, 36, 38}, 30, 40, 37),

        // Range search - Test free number at index 0
        new RangeTest(new int[] {-20, 31, 32, 33, 34, 35, 37, 38, 39, 60, 62}, 30, 50, 30),
        new RangeTest(new int[] {-3, 0, 29, 31, 32, 33, 34, 35, 37, 38, 39, 54, 56}, 30, 40, 30),
        new RangeTest(new int[] {-2, -1, 31, 32, 33, 34, 35, 37, 38, 39, 52, 53}, 0, 50, 0),
        new RangeTest(new int[] {-6, 31, 32, 33, 34, 35, 37, 38, 39, 41}, -5, 40, -5),

        // Range search - Test free number in the middle
        new RangeTest(new int[] {20, 25, 30, 31, 32, 33, 35, 37, 38, 39, 50, 51, 52}, 30, 50, 34),
        new RangeTest(new int[] {15, 30, 31, 32, 33, 34, 37, 38, 39, 40, 41, 42, 43}, 30, 40, 35),
        new RangeTest(new int[] {28, 29, 30, 31, 32, 33, 34, 35, 38, 39, 75, 150}, 30, 50, 36),

        // Range search - Test free number behind the last element
        new RangeTest(new int[] {24, 30, 31, 32, 33, 34, 35, 36, 37, 41, 42}, 30, 40, 38),

        // Range search - Test free number after the first element
        new RangeTest(new int[] {2, 3, 15, 30, 32, 34, 35, 36, 37, 38, 39, 70, 80}, 30, 50, 31),
        new RangeTest(new int[] {0, 1, 30, 33, 34, 35, 36, 37, 38, 39, 40, 41, 45}, 30, 40, 31),

        // Range search - Test free number before the last element
        new RangeTest(new int[] {25, 26, 28, 30, 31, 32, 33, 34, 35, 36, 38, 41, 43}, 30, 40, 37),

        // Range search - Test all occupied elements out of range
        new RangeTest(new int[] {1001, 1002, 1003, 1004, 1005}, 0, 10, 0),
        new RangeTest(new int[] {1001, 1002, 1003, 1004, 1005}, 25, 75, 25),
        new RangeTest(new int[] {15, 16, 17, 18, 19, 20}, 25, 75, 25),
        new RangeTest(new int[] {15, 16, 17, 18, 19, 20, 21, 22, 23, 24}, 25, 75, 25),
        new RangeTest(new int[] {15, 16, 17, 18, 19, 20, 1001, 1002, 1003, 1004, 1005}, 25, 75, 25),
        new RangeTest(new int[] {20, 21, 22, 23, 24, 76, 77, 78, 79, 80}, 25, 75, 25),

        // Range search - Test no occupied elements
        new RangeTest(new int[] {}, 0, 1000, 0),
        new RangeTest(new int[] {}, 30, 40, 30),

        // Range search - Test single element range
        new RangeTest(new int[] {}, 30, 30, 30),
        new RangeTest(new int[] {100, 200}, 120, 120, 120),
        new RangeTest(new int[] {119, 121}, 120, 120, 120),
    };

    public static final RangeTest[] TEST_EXHAUSTED_POOL =
    {
        // Test no free numbers
        new RangeTest(new int[] {30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, 30, 40, 0),

        // Range search - Test no free numbers
        new RangeTest(new int[] {-1, 0, 2, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 44}, 30, 40, 0),
        new RangeTest(new int[] {29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41}, 30, 40, 0),
        new RangeTest(new int[] {29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41}, 34, 36, 0),
        new RangeTest(new int[] {29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41}, 34, 36, 0),

        // Range search - Test single element range
        new RangeTest(new int[] {4}, 4, 4, 0),
        new RangeTest(new int[] {3, 4, 5}, 4, 4, 0),
        new RangeTest(new int[] {2, 4, 6}, 4, 4, 0),
    };

    public NumberAllocTest()
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of getStartIndex method, of class NumberAlloc.
     */
    @Test
    public void testGetStartIndex()
    {
        for (ValueTest curTest : TEST_START_INDEX)
        {
            int result = NumberAlloc.getStartIndex(curTest.occupied, curTest.value);
            if (result != curTest.expectedResult)
            {
                fail(String.format("Result start index %d differs from expected result %d",
                     result, curTest.expectedResult));
            }
        }
    }

    /**
     * Test of getEndIndex method, of class NumberAlloc.
     */
    @Test
    public void testGetEndIndex()
    {
        for (ValueTest curTest : TEST_END_INDEX)
        {
            int result = NumberAlloc.getEndIndex(curTest.occupied, curTest.value);
            if (result != curTest.expectedResult)
            {
                System.out.printf(
                    "TEST { %s }, value = %d\n",
                    Arrays.toString(curTest.occupied), curTest.value
                );
                fail(String.format("Result start index %d differs from expected result %d",
                     result, curTest.expectedResult));
            }
        }
    }

    /**
     * Test of getFreeNumber method, of class NumberAlloc.
     */
    @Test
    public void testGetFreeNumber() throws Exception
    {
        for (RangeTest curTest : TEST_FREE_NUMBER)
        {
            int freeNumber = NumberAlloc.getFreeNumber(
                curTest.occupied, curTest.start, curTest.end
            );
            if (freeNumber != curTest.expectedResult)
            {
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d\n",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(String.format("Free number %d differs from expected result %d",
                     freeNumber, curTest.expectedResult));
            }
        }
    }

    @Test
    public void testExhaustedPool() throws Exception
    {
        for (RangeTest curTest : TEST_EXHAUSTED_POOL)
        {
            try
            {
                int freeNumber = NumberAlloc.getFreeNumber(
                    curTest.occupied, curTest.start, curTest.end
                );
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d\n",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(
                    String.format(
                        "Free number %d allocated from an exhausted pool (pool with not free numbers)",
                        freeNumber
                    )
                );
            }
            catch (ExhaustedPoolException expectedExc)
            {
                // Exception generated as expected
            }
        }
    }

    static class ValueTest
    {
        int[] occupied;
        int value;
        int expectedResult;

        ValueTest(int[] occupiedArg, int valueArg, int expected)
        {
            occupied = occupiedArg;
            value = valueArg;
            expectedResult = expected;
        }
    }

    static class RangeTest
    {
        int[] occupied;
        int start;
        int end;
        int expectedResult;

        RangeTest(int[] occupiedArg, int startArg, int endArg, int expected)
        {
            occupied = occupiedArg;
            start = startArg;
            end = endArg;
            expectedResult = expected;
        }
    }
}
