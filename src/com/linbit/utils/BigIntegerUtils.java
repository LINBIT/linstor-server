package com.linbit.utils;

import java.math.BigInteger;

public class BigIntegerUtils
{
    public static long longValueExact(BigInteger bigint)
    {
        if(bigint.bitLength() > 63)
        {
            throw new ArithmeticException("long overflow");
        }
        return bigint.longValue();
    }
}
