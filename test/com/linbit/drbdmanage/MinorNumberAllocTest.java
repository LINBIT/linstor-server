package com.linbit.drbdmanage;

import com.linbit.ExhaustedPoolException;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Minor number allocation algorithm tests
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MinorNumberAllocTest
{
    public static final MinorAllocTest[] TEST_ALLOC =
    {
        // Test free minor number at index 0
        new MinorAllocTest(new int[] {}, 100, 100, 1000, 100),
        new MinorAllocTest(new int[] { 103, 104, 105, 110, 111, 112, 113, 120 }, 100, 100, 1000, 100),
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 112, 113, 120 }, 100, 100, 1000, 100),

        // Test free minor number at current offset
        new MinorAllocTest(new int[] { 101, 133, 134, 136, 137, 138, 200, 300 }, 135, 100, 1000, 135),
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 132, 133, 134 }, 135, 100, 1000, 135),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 110, 111 }, 135, 100, 1000, 135),

        // Test free minor number at higher numbers
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 132, 133, 134, 135 }, 132, 100, 1000, 136),
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 132, 133, 134, 135, 140 }, 132, 100, 1000, 136),
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 132, 133, 134 }, 132, 100, 1000, 135),
        new MinorAllocTest(new int[] { 101, 102, 104, 108, 111, 132, 133, 134, 140 }, 132, 100, 1000, 135),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 106, 107 }, 102, 100, 1000, 108),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 106, 107, 200 }, 102, 100, 1000, 108),

        // Test free minor number at lower numbers (wrap-around)
        new MinorAllocTest(new int[] { 102, 103, 104, 105, 106, 997, 998, 999, 1000}, 998, 100, 1000, 100),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 106, 997, 998, 999, 1000}, 998, 100, 1000, 104),

        // Range search - Test free minor number at index 0
        new MinorAllocTest(new int[] { 98, 99, 103, 104, 105, 110, 111, 112, 113, 120, 2050 }, 100, 100, 1000, 100),
        new MinorAllocTest(new int[] { 99, 101, 102, 104, 108, 111, 112, 113, 120, 1001 }, 100, 100, 1000, 100),

        // Range search - Test free minor number at current offset
        new MinorAllocTest(new int[] { 0, 10, 134, 136, 137, 138, 200, 300, 2020 }, 135, 100, 1000, 135),
        new MinorAllocTest(new int[] { 97, 98, 101, 102, 132, 133, 134, 1005 }, 135, 100, 1000, 135),
        new MinorAllocTest(new int[] { 99, 100, 101, 102, 103, 104, 105, 1001, 1002 }, 135, 100, 1000, 135),

        // Range search - Test free minor number at higher numbers
        new MinorAllocTest(new int[] { 70, 108, 111, 132, 133, 134, 135, 1001, 1002 }, 132, 100, 1000, 136),
        new MinorAllocTest(new int[] { 70, 108, 111, 132, 133, 134, 135, 140, 1001, 1002 }, 132, 100, 1000, 136),
        new MinorAllocTest(new int[] { 70, 80, 108, 111, 132, 133, 134, 1050 }, 132, 100, 1000, 135),
        new MinorAllocTest(new int[] { 0, 50, 102, 103, 104, 105, 106, 107, 120, 1080 }, 102, 100, 1000, 108),
        new MinorAllocTest(new int[] { 0, 50, 102, 103, 104, 105, 106, 107, 1080 }, 102, 100, 1000, 108),

        // Range search - Test free minor number at lower numbers (wrap-around)
        new MinorAllocTest(new int[] { 97, 98, 99, 102, 103, 997, 998, 999, 1000 }, 998, 100, 1000, 100),
        new MinorAllocTest(new int[] { 0, 98, 99, 100, 101, 102, 103, 105, 999, 1000 }, 999, 100, 1000, 104)
    };

    public static final MinorAllocTest[] TEST_EXHAUSTED_POOL =
    {
        // Exhausted pool (no free minor numbers)
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110 }, 100, 100, 110, 0),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110 }, 110, 100, 110, 0),
        new MinorAllocTest(new int[] { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110 }, 105, 100, 110, 0),
        // Range search - Exhausted pool (no free minor numbers)
        new MinorAllocTest(
            new int[]
            {
                97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113
            },
            100, 100, 110, 0
        ),
        new MinorAllocTest(
            new int[]
            {
                97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113
            },
            105, 100, 110, 0
        ),
        new MinorAllocTest(
            new int[]
            {
                97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113
            },
            110, 100, 110, 0
        ),
        new MinorAllocTest(
            new int[]
            {
                50, 60, 70, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 120, 130, 140
            },
            100, 100, 110, 0
        ),
        new MinorAllocTest(
            new int[]
            {
                50, 60, 70, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 120, 130, 140
            },
            105, 100, 110, 0
        ),
        new MinorAllocTest(
            new int[]
            {
                50, 60, 70, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 120, 130, 140
            },
            110, 100, 110, 0
        ),
    };

    public MinorNumberAllocTest()
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
     * Test of getFreeMinorNumber method, of class MinorNumberAlloc.
     */
    @Test
    public void testGetFreeMinorNumber() throws Exception
    {
        for (MinorAllocTest curTest : TEST_ALLOC)
        {
            MinorNumber freeNumber = MinorNumberAlloc.getFreeMinorNumber(
                curTest.occupied,
                new MinorNumber(curTest.offset),
                new MinorNumber(curTest.start),
                new MinorNumber(curTest.end)
            );
            if (freeNumber.value != curTest.expectedResult)
            {
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d\n",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(String.format("Free number %d differs from expected result %d",
                     freeNumber.value, curTest.expectedResult));
            }
        }
    }

    @Test
    public void testExhaustedPool() throws Exception
    {
        for (MinorAllocTest curTest : TEST_EXHAUSTED_POOL)
        {
            try
            {
                MinorNumber freeNumber = MinorNumberAlloc.getFreeMinorNumber(
                    curTest.occupied,
                    new MinorNumber(curTest.offset),
                    new MinorNumber(curTest.start),
                    new MinorNumber(curTest.end)
                );
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d\n",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(
                    String.format(
                        "Free number %d allocated from an exhausted pool (pool with not free numbers)",
                        freeNumber.value
                    )
                );
            }
            catch (ExhaustedPoolException expectedExc)
            {
                // Exception generated as expected
            }
        }
    }

    static class MinorAllocTest
    {
        int[] occupied;
        int offset;
        int start;
        int end;
        int expectedResult;

        MinorAllocTest(int[] occupiedArg, int offsetArg, int startArg, int endArg, int expected)
        {
            occupied = occupiedArg;
            offset = offsetArg;
            start = startArg;
            end = endArg;
            expectedResult = expected;
        }
    }
}
