package com.linbit.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class SizeUtils
{
    public static final String[] UNITS =
    {
        "kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
    };

    public static String approximateSizeString(long kib)
    {
        return approximateSizeString(BigInteger.valueOf(kib));
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static String approximateSizeString(BigInteger kib)
    {
        final BigInteger maxRemainder = BigInteger.valueOf(1 << 10);
        BigInteger magnitude = BigInteger.valueOf(1);
        int unitIdx = 0;
        final int maxUnitIdx = UNITS.length - 1;
        // If the size value in the current unit is less than 1024, then
        // use the current unit, otherwise increase the magnitude and use
        // the next larger unit
        while (kib.divide(magnitude).compareTo(maxRemainder) >= 0 && unitIdx < maxUnitIdx)
        {
            magnitude = magnitude.shiftLeft(10);
            ++unitIdx;
        }

        BigDecimal kibDec = new BigDecimal(kib);
        float sizeUnit = kibDec.divide(new BigDecimal(magnitude), 2, RoundingMode.CEILING).floatValue();
        String sizeStr = String.format("%4.2f %s", sizeUnit, UNITS[unitIdx]);

        return sizeStr;
    }

    private SizeUtils()
    {
    }
}
