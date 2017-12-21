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

    public static <T extends Comparable<T>> T bounds(
        final T lowerBound,
        final T value,
        final T upperBound
    )
    {
        T result = value;
        if (value.compareTo(lowerBound) < 0)
        {
            result = lowerBound;
        }
        else
        if (value.compareTo(upperBound) > 0)
        {
            result = upperBound;
        }
        return result;
    }

    private MathUtils()
    {
    }
}
