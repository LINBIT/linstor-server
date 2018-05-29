package com.linbit.utils;

public class MathUtils
{
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
