package com.linbit.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class SizeUtils
{
    public static final String[] units =
    {
        "kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
    };

    public static String approximateSizeString(long kib)
    {
        return approximateSizeString(BigInteger.valueOf(kib));
    }

    public static String approximateSizeString(BigInteger kib)
    {
        int index = 0;
        int counter = 1;
        BigInteger magnitute = BigInteger.valueOf(1 << 10);
        while (counter < units.length)
        {
            if (kib.compareTo(magnitute) >= 0)
            {
                index = counter;
            }
            else
            {
                break;
            }
            magnitute = magnitute.shiftLeft(10);
            counter++;
        }
        magnitute = magnitute.shiftRight(10);

        String sizeStr;
        // if (kib.mod(magnitute).compareTo(BigInteger.ZERO) != 0) // kib % magnitute != 0
        // {
        BigDecimal kibDec = new BigDecimal(kib);
        float sizeUnit = kibDec.divide(new BigDecimal(magnitute), 2, RoundingMode.CEILING).floatValue();
        sizeStr = String.format("%3.2f %s", sizeUnit, units[index]);
        // }

        return sizeStr;
    }

}
