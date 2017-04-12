package com.linbit.utils;

import java.math.BigInteger;

public class BigIntegerUtils
{
    public static long longValueExact(BigInteger bigInt)
    {
        if (bigInt.bitLength() > 63)
        {
            throw new ArithmeticException("long overflow");
        }
        return bigInt.longValue();
    }
}
