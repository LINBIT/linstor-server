package com.linbit;

import com.linbit.SizeConv.SizeUnit;

import static com.linbit.SizeConv.SizeUnit.UNIT_B;
import static com.linbit.SizeConv.SizeUnit.UNIT_EB;
import static com.linbit.SizeConv.SizeUnit.UNIT_EiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_GB;
import static com.linbit.SizeConv.SizeUnit.UNIT_GiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_KiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_MB;
import static com.linbit.SizeConv.SizeUnit.UNIT_MiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_PB;
import static com.linbit.SizeConv.SizeUnit.UNIT_PiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_TB;
import static com.linbit.SizeConv.SizeUnit.UNIT_TiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_YB;
import static com.linbit.SizeConv.SizeUnit.UNIT_YiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_ZB;
import static com.linbit.SizeConv.SizeUnit.UNIT_ZiB;
import static com.linbit.SizeConv.SizeUnit.UNIT_kB;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for size conversion algorithms
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SizeConvTest
{
    static final TestEntry[] TEST_DATA =
    {
        new TestEntry(1L, UNIT_B, UNIT_B, 1L, false),
        new TestEntry(1L, UNIT_kB, UNIT_kB, 1L, false),
        new TestEntry(1L, UNIT_MB, UNIT_MB, 1L, false),
        new TestEntry(1L, UNIT_GB, UNIT_GB, 1L, false),
        new TestEntry(1L, UNIT_TB, UNIT_TB, 1L, false),
        new TestEntry(1L, UNIT_PB, UNIT_PB, 1L, false),
        new TestEntry(1L, UNIT_EB, UNIT_EB, 1L, false),
        new TestEntry(1L, UNIT_ZB, UNIT_ZB, 1L, false),
        new TestEntry(1L, UNIT_YB, UNIT_YB, 1L, false),
        new TestEntry(1L, UNIT_KiB, UNIT_KiB, 1L, false),
        new TestEntry(1L, UNIT_MiB, UNIT_MiB, 1L, false),
        new TestEntry(1L, UNIT_GiB, UNIT_GiB, 1L, false),
        new TestEntry(1L, UNIT_TiB, UNIT_TiB, 1L, false),
        new TestEntry(1L, UNIT_PiB, UNIT_PiB, 1L, false),
        new TestEntry(1L, UNIT_EiB, UNIT_EiB, 1L, false),
        new TestEntry(1L, UNIT_ZiB, UNIT_ZiB, 1L, false),
        new TestEntry(1L, UNIT_YiB, UNIT_YiB, 1L, false),
        new TestEntry(1L, UNIT_kB, UNIT_B, 1000L, false),
        new TestEntry(1L, UNIT_MB, UNIT_B, 1000000L, false),
        new TestEntry(1L, UNIT_GB, UNIT_B, 1000000000L, false),
        new TestEntry(1L, UNIT_TB, UNIT_B, 1000000000000L, false),
        new TestEntry(1L, UNIT_PB, UNIT_B, 1000000000000000L, false),
        new TestEntry(1L, UNIT_EB, UNIT_B, 1000000000000000000L, false),
        new TestEntry(1L, UNIT_ZB, UNIT_kB, 1000000000000000000L, false),
        new TestEntry(1L, UNIT_YB, UNIT_MB, 1000000000000000000L, false),
        new TestEntry(1L, UNIT_KiB, UNIT_B, 1024L, false),
        new TestEntry(1L, UNIT_MiB, UNIT_B, 1048576L, false),
        new TestEntry(1L, UNIT_GiB, UNIT_B, 1073741824L, false),
        new TestEntry(1L, UNIT_TiB, UNIT_B, 1099511627776L, false),
        new TestEntry(1L, UNIT_PiB, UNIT_B, 1125899906842624L, false),
        new TestEntry(1L, UNIT_EiB, UNIT_B, 1152921504606846976L, false),
        new TestEntry(1L, UNIT_ZiB, UNIT_KiB, 1152921504606846976L, false),
        new TestEntry(1L, UNIT_YiB, UNIT_MiB, 1152921504606846976L, false),
        new TestEntry(1L, UNIT_MiB, UNIT_B, 1048576L, false),
        new TestEntry(1L, UNIT_MB, UNIT_KiB, 976L, true),
        new TestEntry(262144L, UNIT_KiB, UNIT_MiB, 256L, false),
        new TestEntry(63L, UNIT_GiB, UNIT_KiB, 66060288L, false),
        new TestEntry(40516976640L, UNIT_MiB, UNIT_TiB, 38640L, false),
        new TestEntry(48510, UNIT_GB, UNIT_KiB, 47373046875L, false),
        new TestEntry(1, UNIT_KiB, UNIT_MiB, 0, true),
        new TestEntry(1023, UNIT_KiB, UNIT_MiB, 0, true),
        new TestEntry(2047, UNIT_KiB, UNIT_MiB, 1, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_KiB, 56907700850L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_MiB, 55573926L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_GiB, 54271L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_TiB, 52L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_kB, 58273485671L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_MB, 58273485L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_GB, 58273L, true),
        new TestEntry(58273485671283L, UNIT_B, UNIT_TB, 58L, true),
        new TestEntry(13454L, UNIT_PiB, UNIT_TiB, 13776896L, false),
        new TestEntry(13454L, UNIT_EiB, UNIT_TiB, 14107541504L, false),
        new TestEntry(13454L, UNIT_ZiB, UNIT_TiB, 14446122500096L, false),
        new TestEntry(13454L, UNIT_YiB, UNIT_TiB, 14792829440098304L, false),
        new TestEntry(13454L, UNIT_PiB, UNIT_TB, 15147857L, true),
        new TestEntry(13454L, UNIT_EiB, UNIT_TB, 15511405922L, true),
        new TestEntry(13454L, UNIT_ZiB, UNIT_TB, 15883679665132L, true),
        new TestEntry(13454L, UNIT_YiB, UNIT_TB, 16264887977095220L, true),
        new TestEntry(17641L, UNIT_PB, UNIT_TB, 17641000L, false),
        new TestEntry(17641L, UNIT_EB, UNIT_TB, 17641000000L, false),
        new TestEntry(17641L, UNIT_ZB, UNIT_TB, 17641000000000L, false),
        new TestEntry(17641L, UNIT_YB, UNIT_TB, 17641000000000000L, false),
        new TestEntry(17641L, UNIT_PB, UNIT_TiB, 16044396L, true),
        new TestEntry(17641L, UNIT_EB, UNIT_TiB, 16044396033L, true),
        new TestEntry(17641L, UNIT_ZB, UNIT_TiB, 16044396033976L, true),
        new TestEntry(17641L, UNIT_YB, UNIT_TiB, 16044396033976227L, true),
        new TestEntry(53157, UNIT_YiB, UNIT_TB, 64262869793254843L, true)
    };

    public SizeConvTest()
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
     * Test of convert method, of class SizeConv.
     */
    @Test
    public void testConvert()
    {
        for (int index = 0; index < TEST_DATA.length; ++index)
        {
            TestEntry entry = TEST_DATA[index];
            try
            {
                long result = SizeConv.convert(entry.size, entry.unitIn, entry.unitOut);
                if (result != entry.expectedResult)
                {
                    printConstants();
                    fail(
                        String.format(
                            "Test %d failed, input size(%d), unitIn(%s), unitOut(%s), " +
                            "expectedResult(%d), calculation result(%d)",
                            index, entry.size, entry.unitIn, entry.unitOut, entry.expectedResult, result
                        )
                    );
                }
            }
            catch (ArithmeticException arithExc)
            {
                printConstants();
                System.err.printf(
                    "Test %d failed, input size(%d), unitIn(%s), unitOut(%s), expectedResult(%d)",
                    index, entry.size, entry.unitIn, entry.unitOut, entry.expectedResult
                );
                throw arithExc;
            }
        }
    }

    /**
     * Test of convert method, of class SizeConv.
     */
    @Test
    public void testConvertRoundUp()
    {
        for (int index = 0; index < TEST_DATA.length; ++index)
        {
            TestEntry entry = TEST_DATA[index];
            try
            {
                long result = SizeConv.convertRoundUp(entry.size, entry.unitIn, entry.unitOut);
                long expectedResult = entry.expectedRounded ? entry.expectedResult + 1 : entry.expectedResult;
                if (result != expectedResult)
                {
                    printConstants();
                    fail(
                        String.format(
                            "Test %d failed, input size(%d), unitIn(%s), unitOut(%s), " +
                            "expectedResult(%d), calculation result(%d)",
                            index, entry.size, entry.unitIn, entry.unitOut, entry.expectedResult, result
                        )
                    );
                }
            }
            catch (ArithmeticException arithExc)
            {
                printConstants();
                System.err.printf(
                    "Test %d failed, input size(%d), unitIn(%s), unitOut(%s), expectedResult(%d)",
                    index, entry.size, entry.unitIn, entry.unitOut, entry.expectedResult
                );
                throw arithExc;
            }
        }
    }

    @Test
    public void testParse() throws Exception
    {
        ParseEntry[] entries = new ParseEntry[] {
            new ParseEntry(SizeUnit.UNIT_B, SizeUnit.UNIT_B, false, "", "b", "B"),
            new ParseEntry(SizeUnit.UNIT_kB, SizeUnit.UNIT_KiB, false, "k", "kib", "kb", "KB", "KiB", "kiB", "kIb"),
            new ParseEntry(SizeUnit.UNIT_MB, SizeUnit.UNIT_MiB, false, "m", "mb", "mib", "MB", "MiB", "miB", "mIb"),
            new ParseEntry(SizeUnit.UNIT_GB, SizeUnit.UNIT_GiB, false, "g", "gb", "gib", "GB", "GiB", "giB", "gIb"),
            new ParseEntry(SizeUnit.UNIT_TB, SizeUnit.UNIT_TiB, false, "t", "tb", "tib", "TB", "TiB", "tiB", "tIb"),
            new ParseEntry(SizeUnit.UNIT_PB, SizeUnit.UNIT_PiB, false, "p", "pb", "pib", "PB", "PiB", "piB", "pIb"),
            new ParseEntry(SizeUnit.UNIT_EB, SizeUnit.UNIT_EiB, false, "e", "eb", "eib", "EB", "EiB", "eiB", "eIb"),
            new ParseEntry(SizeUnit.UNIT_ZB, SizeUnit.UNIT_ZiB, false, "z", "zb", "zib", "ZB", "ZiB", "ziB", "zIb"),
            new ParseEntry(SizeUnit.UNIT_YB, SizeUnit.UNIT_YiB, false, "y", "yb", "yib", "YB", "YiB", "yiB", "yIb"),
            new ParseEntry(null, null, true, "a", "cb", "gibb", "gi b", "gbi", "9001"),
        };

        for (ParseEntry entry : entries)
        {
            for (String str : entry.strArr)
            {
                if (entry.expectException)
                {
                    try
                    {
                        SizeUnit.parse(str, false);
                        fail("Exception expected at: " + str + ", force^2: false");
                    }
                    catch (IllegalArgumentException ignored)
                    {
                    }
                    try
                    {
                        SizeUnit.parse(str, true);
                        fail("Exception expected at: " + str + ", force^2: true");
                    }
                    catch (IllegalArgumentException ignored)
                    {
                    }
                }
                else
                {
                    SizeUnit expectedBaseUnit = str.toLowerCase().contains("i") ?
                        entry.expectedPowerTwoUnit :
                        entry.expectedUnit;
                    assertEquals(expectedBaseUnit, SizeUnit.parse(str, false));
                    assertEquals(entry.expectedPowerTwoUnit, SizeUnit.parse(str, true));
                }
            }
        }
    }

    public static void printConstants()
    {
        System.out.println("== Begin: Constants dump ==");
        System.out.println("FACTOR_B = " + SizeConv.FACTOR_B);
        System.out.println("FACTOR_kB = " + SizeConv.FACTOR_kB);
        System.out.println("FACTOR_MB = " + SizeConv.FACTOR_MB);
        System.out.println("FACTOR_GB = " + SizeConv.FACTOR_GB);
        System.out.println("FACTOR_TB = " + SizeConv.FACTOR_TB);
        System.out.println("FACTOR_PB = " + SizeConv.FACTOR_PB);
        System.out.println("FACTOR_EB = " + SizeConv.FACTOR_EB);
        System.out.println("FACTOR_ZB = " + SizeConv.FACTOR_ZB);
        System.out.println("FACTOR_YB = " + SizeConv.FACTOR_YB);
        System.out.println("FACTOR_kiB = " + SizeConv.FACTOR_KiB);
        System.out.println("FACTOR_MiB = " + SizeConv.FACTOR_MiB);
        System.out.println("FACTOR_GiB = " + SizeConv.FACTOR_GiB);
        System.out.println("FACTOR_TiB = " + SizeConv.FACTOR_TiB);
        System.out.println("FACTOR_PiB = " + SizeConv.FACTOR_PiB);
        System.out.println("FACTOR_EiB = " + SizeConv.FACTOR_EiB);
        System.out.println("FACTOR_ZiB = " + SizeConv.FACTOR_ZiB);
        System.out.println("FACTOR_YiB = " + SizeConv.FACTOR_YiB);
        System.out.println("== End: Constants dump ==\n");
    }

    static class TestEntry
    {
        TestEntry(
            long sizeRef,
            SizeConv.SizeUnit unitInRef,
            SizeConv.SizeUnit unitOutRef,
            long expectedResultRef,
            boolean expectedRoundedRef
        )
        {
            size = sizeRef;
            unitIn = unitInRef;
            unitOut = unitOutRef;
            expectedResult = expectedResultRef;
            expectedRounded = expectedRoundedRef;
        }

        long size;
        SizeConv.SizeUnit unitIn;
        SizeConv.SizeUnit unitOut;
        long expectedResult;
        boolean expectedRounded;
    }

    static class ParseEntry
    {
        SizeUnit expectedUnit;
        SizeUnit expectedPowerTwoUnit;
        boolean expectException;
        String[] strArr;

        ParseEntry(SizeUnit unitRef, SizeUnit expectedPowerTwoUnitRef, boolean expectExceptionRef, String... strArrRef)
        {
            expectedUnit = unitRef;
            expectedPowerTwoUnit = expectedPowerTwoUnitRef;
            expectException = expectExceptionRef;
            strArr = strArrRef;
        }
    }
}
