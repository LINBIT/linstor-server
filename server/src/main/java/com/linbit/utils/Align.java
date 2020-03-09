package com.linbit.utils;

public class Align
{
    public final long alignBase;
    public final long maxAlignedValue;

    public Align(final long base)
    {
        if (base < 1)
        {
            throw new IllegalArgumentException("Invalid base value for alignment, base = " + base);
        }

        alignBase = base;
        maxAlignedValue = (Long.MAX_VALUE / base) * base;
    }

    public long floor(final long value)
    {
        if (value < 0)
        {
            throw new ArithmeticException("Alignment of negative values is not supported");
        }

        return (value / alignBase) * alignBase;
    }

    public long ceiling(final long value)
    {
        if (value < 0)
        {
            throw new ArithmeticException("Alignment of negative values is not supported");
        }

        long result = value;
        if (result <= maxAlignedValue)
        {
            if (result % alignBase != 0)
            {
                result = ((result / alignBase) + 1) * alignBase;
            }
        }
        else
        {
            throw new ArithmeticException(
                "Input value is out of range for ceiling alignment, " +
                "value = " + value + ", base = " + alignBase
            );
        }
        return result;
    }
}
