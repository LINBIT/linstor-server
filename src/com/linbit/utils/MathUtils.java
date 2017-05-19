package com.linbit.utils;

public class MathUtils
{
    public static long addExact(long a, long b)
    {
        if (Long.MAX_VALUE - a < b)
        {
            throw new ArithmeticException("Addition overflows the maximum value for data type long");
        }
        return a + b;
    }

    private MathUtils()
    {
    }
}
