package com.linbit.drbdmanage;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueOutOfRangeException;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests TCP port number allocation algorithms
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpPortNumberAllocTest
{
    public static final PortTest[] TEST_FREE_NUMBER =
    {
        // Test free number at index 0
        new PortTest(new int[] { 7002, 7003, 7004, 7005, 7007, 7008, 7012, 7013, 7015 }, 7000, 7015, 7000),
        new PortTest(new int[] { 7001, 7002, 7003, 7004, 7006, 7008, 7012, 7013, 7015 }, 7000, 7015, 7000),
        new PortTest(new int[] { 1, 2, 3, 5, 7, 8, 12, 613, 614, 615, 721, 731, 7015 }, 0, 65535, 0),

        // Test free number in the middle
        new PortTest(new int[] { 7000, 7001, 7002, 7003, 7006, 7007, 7008, 7010, 7011 }, 7000, 7015, 7004),
        new PortTest(new int[] { 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7010, 7011 }, 7000, 7015, 7007),

        // Test free number behind the last element
        new PortTest(new int[] { 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 7008 }, 7000, 7015, 7009),

        // Test free number after the first element
        new PortTest(new int[] { 7000, 7002, 7003, 7004, 7005, 7006, 7008, 7009, 7015 }, 7000, 7015, 7001),

        // Test free number before the last element
        new PortTest(new int[] { 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 7015 }, 7000, 7015, 7008),

        // Range search - Test free number in the middle
        new PortTest(new int[] { 6999, 7000, 7001, 7002, 7003, 7006, 7007, 7008, 7010, 7011 }, 7000, 7015, 7004),
        new PortTest(new int[] { 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7010, 7011, 7016 }, 7000, 7015, 7007),

        // Range search - Test free number behind the last element
        new PortTest(
            new int[] { 0, 15, 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 7008, 9000, 9015 },
            7000, 7015, 7009
        ),

        // Range search - Test free number after the first element
        new PortTest(
            new int[] { 6998, 7000, 7002, 7003, 7004, 7005, 7006, 7008, 7009, 7015, 8000, 8001, 8002 },
            7000, 7015, 7001
        ),

        // Range search - Test free number before the last element
        new PortTest(
            new int[] { 6900, 6950, 7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 7015, 7016 },
            7000, 7015, 7008
        ),

        // Range search - Test all occupied elements out of range
        new PortTest(
            new int[] { 7000, 7002, 7003, 7004, 7005, 7006, 7008, 7009, 7015 },
            8000, 9000, 8000
        ),
        new PortTest(
            new int[] { 7000, 7002, 7003, 7004, 7005, 7006, 7008, 7009, 7015 },
            5000, 6000, 5000
        ),
        new PortTest(
            new int[] { 7000, 7002, 7003, 7004, 7005, 8000, 8001, 8002, 8003 },
            7400, 7600, 7400
        ),

        // Range search - Test no occupied elements
        new PortTest(new int[] { }, 600, 900, 600),

        // Range search - Test single element range
        new PortTest(new int[] { 2001, 2002, 2003, 2004 }, 2000, 2000, 2000),
        new PortTest(new int[] { 2001, 2002, 2003, 2004 }, 2005, 2005, 2005),
        new PortTest(new int[] { 2001 }, 2002, 2002, 2002),
        new PortTest(new int[] { 2002 }, 2001, 2001, 2001),
        new PortTest(new int[] { }, 2000, 2000, 2000)
    };

    public static final PortTest[] TEST_EXHAUSTED_POOL =
    {
        // Test no free numbers
        new PortTest(new int[] { 4000, 4001, 4002, 4003, 4004, 4005 }, 4000, 4005, 0),

        // Range search - Test no free numbers
        new PortTest(
            new int[] { 3998, 3999, 4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008 },
            4000, 4005, 0
        ),
        new PortTest(
            new int[] { 3995, 3997, 4000, 4001, 4002, 4003, 4004, 4005, 4007, 4008, 4010 },
            4000, 4005, 0
        ),
        new PortTest(
            new int[] { -15, -5, 0, 15, 4000, 4001, 4002, 4003, 4004, 4005, 65535 },
            4000, 4005, 0
        ),

        // Range search - Test single element range
        new PortTest(new int[] { 9000 }, 9000, 9000, 0),
        new PortTest(new int[] { 8999, 9000, 9001 }, 9000, 9000, 0),
        new PortTest(new int[] { 8998, 9000, 9002 }, 9000, 9000, 0),
    };

    public TcpPortNumberAllocTest()
    {
    }

    /**
     * Test of getFreeMinorNumber method, of class TcpPortNumberAlloc.
     */
    @Test
    public void testGetFreePortNumber()
        throws ExhaustedPoolException, ValueOutOfRangeException
    {
        for (PortTest curTest : TEST_FREE_NUMBER)
        {
            TcpPortNumber freeNumber = TcpPortNumberAlloc.getFreePortNumber(
                curTest.occupied,
                new TcpPortNumber(curTest.start),
                new TcpPortNumber(curTest.end)
            );
            if (freeNumber.value != curTest.expectedResult)
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
    public void testExhaustedPool()
        throws ValueOutOfRangeException
    {
        for (PortTest curTest : TEST_EXHAUSTED_POOL)
        {
            try
            {
                TcpPortNumber freeNumber = TcpPortNumberAlloc.getFreePortNumber(
                    curTest.occupied,
                    new TcpPortNumber(curTest.start),
                    new TcpPortNumber(curTest.end)
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

    static class PortTest
    {
        int[] occupied;
        int start;
        int end;
        int expectedResult;

        PortTest(int[] occupiedArg, int startArg, int endArg, int expected)
        {
            occupied = occupiedArg;
            start = startArg;
            end = endArg;
            expectedResult = expected;
        }
    }
}
