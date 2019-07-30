package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests Volume number allocation algorithms
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeNumberAllocTest
{
    public static final VolumeNrTest[] TEST_FREE_NUMBER =
    {
        // Test free number at index 0
        new VolumeNrTest(new int[] {11, 12, 13, 14, 16, 17, 19, 20, 24, 25, 26, 30}, 10, 20, 10),
        new VolumeNrTest(new int[] {12, 13, 14, 15, 16, 17, 19, 20, 24, 25, 26, 30}, 10, 1023, 10),
        new VolumeNrTest(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 15, 0),

        // Test free number in the middle
        new VolumeNrTest(new int[] {100, 101, 102, 103, 104, 106, 107, 108, 109}, 100, 200, 105),
        new VolumeNrTest(new int[] {100, 101, 102, 103, 104, 105, 106, 112, 113, 114}, 100, 200, 107),

        // Test free number behind the last element
        new VolumeNrTest(new int[] {100, 101, 102, 103, 104, 105}, 100, 200, 106),

        // Test free number after the first element
        new VolumeNrTest(new int[] {100, 102, 103, 104, 105, 106, 108, 109, 115}, 100, 200, 101),
        new VolumeNrTest(new int[] {100, 105, 106, 108, 109, 115}, 100, 200, 101),

        // Test free number before the last element
        new VolumeNrTest(new int[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 60}, 50, 90, 59),
        new VolumeNrTest(new int[] {50, 51, 52, 53, 54, 55, 56, 57, 58, 70}, 50, 90, 59),

        // Range search - Test free number in the middle
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15}, 3, 13, 10),
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15}, 4, 12, 8),

        // Range search - Test free number behind the last element
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20}, 3, 15, 14),

        // Range search - Test free number after the first element
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 5, 10, 6),

        // Range search - Test free number before the last element
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15}, 5, 10, 9),

        // Range search - Test all occupied elements out of range
        new VolumeNrTest(new int[] {10, 11, 12, 13, 14, 15}, 0, 9, 0),
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 10, 15, 10),
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 11, 12, 13, 14, 15}, 5, 10, 5),

        // Range search - Test no occupied elements
        new VolumeNrTest(new int[] {}, 0, 100, 0),
        new VolumeNrTest(new int[] {}, 300, 30000, 300),

        // Range search - Test single element range
        new VolumeNrTest(new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 0, 0),
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 15, 15, 15),
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15}, 10, 10, 10),
        new VolumeNrTest(new int[] {4}, 5, 5, 5),
        new VolumeNrTest(new int[] {6}, 5, 5, 5),
        new VolumeNrTest(new int[] {}, 0, 0, 0),
        new VolumeNrTest(new int[] {}, 199, 199, 199)
    };

    public static final VolumeNrTest[] TEST_EXHAUSTED_POOL =
    {
        // Test no free numbers
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 0, 15, 0),

        // Range search - Test no free numbers
        new VolumeNrTest(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, 5, 10, 0),
        new VolumeNrTest(new int[] {0, 1, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15}, 5, 10, 0),

        // Range search - Test single element range
        new VolumeNrTest(new int[] {0}, 0, 0, 0),
        new VolumeNrTest(new int[] {70}, 70, 70, 0),
        new VolumeNrTest(new int[] {69, 70, 71}, 70, 70, 0),
        new VolumeNrTest(new int[] {20, 25, 30, 35}, 25, 25, 0)
    };

    @Test
    public void getFreeVolumeNumber()
        throws ExhaustedPoolException, ValueOutOfRangeException
    {
        for (VolumeNrTest curTest : TEST_FREE_NUMBER)
        {
            VolumeNumber freeNumber = VolumeNumberAlloc.getFreeVolumeNumber(
                curTest.occupied,
                new VolumeNumber(curTest.start),
                new VolumeNumber(curTest.end)
            );
            if (freeNumber.value != curTest.expectedResult)
            {
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d\n",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(
                    String.format(
                        "Free number %d differs from expected result %d",
                        freeNumber, curTest.expectedResult
                    )
                );
            }
        }
    }

    @Test
    public void testExhaustedPool()
        throws ValueOutOfRangeException
    {
        for (VolumeNrTest curTest : TEST_EXHAUSTED_POOL)
        {
            try
            {
                VolumeNumber freeNumber = VolumeNumberAlloc.getFreeVolumeNumber(
                    curTest.occupied,
                    new VolumeNumber(curTest.start),
                    new VolumeNumber(curTest.end)
                );
                System.out.printf(
                    "TEST { %s }, start = %d, end = %d",
                    Arrays.toString(curTest.occupied), curTest.start, curTest.end
                );
                fail(
                    String.format(
                        "Free number %d allocated from an exhausted pool (pool with no free numbers)",
                        freeNumber.value
                    )
                );
            }
            catch (ExhaustedPoolException expectedExc)
            {
                // Exception generated as expected, nothing to do
            }
        }
    }

    static class VolumeNrTest
    {
        int[] occupied;
        int start;
        int end;
        int expectedResult;

        VolumeNrTest(int[] occupiedArg, int startArg, int endArg, int expected)
        {
            occupied = occupiedArg;
            start = startArg;
            end = endArg;
            expectedResult = expected;
        }
    }
}
