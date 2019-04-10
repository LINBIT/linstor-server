package com.linbit.utils;


import java.math.BigInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SizeUtilsTest
{
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testApproxSizeAllSupportedMagnitudes()
    {
        BigInteger kib = BigInteger.valueOf(1);
        for (String magnitude : SizeUtils.UNITS)
        {
            assertEquals("1.00 " + magnitude, SizeUtils.approximateSizeString(kib));
            kib = kib.shiftLeft(9);
            assertEquals("512.00 " + magnitude, SizeUtils.approximateSizeString(kib));
            kib = kib.shiftLeft(1);
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testApproxSizeDecimal()
    {
        checkApprox(102400, "100.00 MiB");
        checkApprox(102502, "100.10 MiB");
        checkApprox(102512, "100.11 MiB");
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testApproxSizeRounding()
    {
        checkApprox(102400, "100.00 MiB");
        checkApprox(102399, "100.00 MiB"); // rounded up
        // we do not check around 89 or 90 where the actual rounding happes because of rounding errors
        // 85 should be far away from that edge
        checkApprox(102385, "99.99 MiB");
    }

    private void checkApprox(long kib, String expected)
    {
        assertEquals(expected, SizeUtils.approximateSizeString(kib));
    }
}
