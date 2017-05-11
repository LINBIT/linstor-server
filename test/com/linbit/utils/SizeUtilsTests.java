package com.linbit.utils;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Test;

public class SizeUtilsTests
{
    @Test
    public void testApproxSizeAllSupportedMagnitutes()
    {
        BigInteger kib = BigInteger.valueOf(1);
        for (String magnitute : SizeUtils.units)
        {
            assertEquals("1.00 " + magnitute, SizeUtils.approximateSizeString(kib));
            kib = kib.shiftLeft(9);
            assertEquals("512.00 " + magnitute, SizeUtils.approximateSizeString(kib));
            kib = kib.shiftLeft(1);
        }
    }

    @Test
    public void testApproxSizeDecimal()
    {
        checkApprox(102400, "100.00 MiB");
        checkApprox(102502, "100.10 MiB");
        checkApprox(102512, "100.11 MiB");
    }

    @Test
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
