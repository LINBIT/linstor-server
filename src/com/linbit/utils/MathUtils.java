package com.linbit.utils;

public class MathUtils
{
    public static long addExact(long a, long b)
    {
        if(Long.MAX_VALUE - a < b)
        {
            throw new ArithmeticException("long overflow");
        }
        // else ...
        return a + b;
    }
}
