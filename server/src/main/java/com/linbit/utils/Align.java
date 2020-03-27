package com.linbit.utils;

/**
 * Aligns positive values to the specified alignment.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Align
{
    public final long alignBase;
    public final long maxAlignedValue;

    /**
     * Constructs a new instance of the {@code Align} class with the specified {@code base}.
     *
     * The specified {@code base} must be a positive value.
     *
     * Values aligned by the new instance will be aligned so that the result
     * is a multiple of the specified base.
     *
     * @param base Base value for the alignment of values
     * @throws IllegalArgumentException If the specified {@code base} is less than 1
     */
    public Align(final long base)
    {
        if (base < 1)
        {
            throw new IllegalArgumentException("Invalid base value for alignment, base = " + base);
        }

        alignBase = base;
        maxAlignedValue = (Long.MAX_VALUE / base) * base;
    }

    /**
     * Returns the specified positive {@code value} floor-aligned according to the {@code alignBase}
     * of this {@code Align} instance.
     *
     * The returned aligned value will be less than or equal to the specified value.
     *
     * @param value The value to align
     * @return Aligned value
     * @throws ArithmeticException If a negative {@code value} is specified
     */
    public long floor(final long value)
    {
        if (value < 0)
        {
            throw new ArithmeticException("Alignment of negative values is not supported");
        }

        return (value / alignBase) * alignBase;
    }

    /**
     * Returns the specified positive {@code value} ceiling-aligned according to the {@code alignBase}
     * of this {@code Align} instance.
     *
     * The returned aligned value will be greater than or equal to the specified value.
     *
     * @param value The value to align
     * @return Aligned value
     * @throws ArithmeticException If a negative {@code value} is specified or
     *                             the specified {@code value} is out of range
     */
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
