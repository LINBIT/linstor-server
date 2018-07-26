package com.linbit.utils;

import java.math.BigInteger;

public class BigIntegerUtils
{
    public static long longValueExact(BigInteger bigInt)
    {
        if (bigInt.bitLength() >= Long.SIZE)
        {
            throw new ArithmeticException("Input value for conversion is not within the range of data type long");
        }
        return bigInt.longValue();
    }

    private BigIntegerUtils()
    {
    }
}
