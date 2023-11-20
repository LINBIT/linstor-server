package com.linbit.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MathUtils
{
    private static final int[] PRIMES_TO_100 =
    {
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41,
        43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    };

    private static final int INTR_CHECK_FREQ    = 10000;

    /**
     * See <code>longFloorSqrt</code>
     */
    public static long longSqrt(final long value)
    {
        return longFloorSqrt(value);
    }

    /**
     * Returns the square root of the specified value.
     * The result returned is the largest integer value that is equal to or less than the square root of
     * the specified value.
     *
     * @param value Input value for square root calculation
     * @return Largest value equal to or less than the square root of the input value
     */
    public static long longFloorSqrt(final long value)
    {
        long base = 0;
        if (value > 0)
        {
            long mask = (1L << 31);
            do
            {
                long probe = base | mask;
                if (probe <= 0xB504F333L)
                {
                    final long probeSquared = probe * probe;
                    if (probeSquared <= value)
                    {
                        base |= probe;
                        if (probeSquared == value)
                        {
                            break;
                        }
                    }
                }
                mask >>= 1;
            }
            while (mask > 0);
        }
        else
        if (value < 0)
        {
            throw new ArithmeticException("Complex square root calculation is not implemented");
        }
        return base;
    }

    /**
     * Returns the ceiling value of the square root of the specified value.
     * The result returned is the smallest integer value that is equal to or greater than the square root of
     * the specified value.
     *
     * @param value Input value for square root calculation
     * @return Largest value equal to or less than the square root of the input value
     */
    public static long longCeilingSqrt(final long value)
    {
        final long sqrtVal = longFloorSqrt(value);
        final long result = (sqrtVal * sqrtVal < value ? sqrtVal + 1 : sqrtVal);
        return result;
    }

    /**
     * Returns the result of calculating <code>base</code> to the power of <code>exponent</code>.
     *
     * @param base Base value
     * @param exponent Exponent for calculating the power of the base calue
     * @return <code>base</code> to the power of <code>exponent</code>
     */
    public static long longPower(final long base, final int exponent)
    {
        if (exponent < 0 || exponent >= 63)
        {
            throw new ArithmeticException("Exponent " + exponent + " is out of range");
        }

        long product = 1;
        for (long ctr = 0; ctr < exponent; ++ctr)
        {
            if (base == 0 || Long.MAX_VALUE / base >= base)
            {
                product *= base;
            }
            else
            {
                throw new ArithmeticException("Power " + exponent + " of " + base + " is out of range");
            }
        }
        return product;
    }

    /**
     * Calculates and stores the prime factor (always 2 in this case) and exponent of the greatest power of 2 number
     * in the specified set of numbers.
     * <br/><br/>
     * From all positive powers of 2 in the specified set of numbers, this method will find the greatest number and
     * determine its exponent. An entry for base 2 and the calculated exponent is stored in the
     * <code>primeFactors</code> map.<br/>
     * If <code>remaining</code> is non-null, all numbers that are not positive powers of 2 are stored in the
     * <code>remaining</code> set.<br/>
     * <br/>
     * The number 1 is not considered to be a power of 2.
     *
     * @param numbers Set of input numbers
     * @param remaining If non-null, receives the set of numbers that are not positive powers of 2
     * @param primeFactors Map of calculated base / exponent entries
     */
    public static void fastPower2Factorize(
        @Nonnull final Set<Long>            numbers,
        @Nullable final Set<Long>           remaining,
        @Nonnull final Map<Long, Integer>   primeFactors
    )
    {
        long ceilValue = 0;
        for (Long value : numbers)
        {
            // All positive powers of two, and only powers of two, are positive numbers with a hamming weight of 1
            if (value > 1 && Long.bitCount(value) == 1)
            {
                if (value > ceilValue)
                {
                    ceilValue = value;
                }
            }
            else
            if (remaining != null)
            {
                remaining.add(value);
            }
        }

        int exponent = 0;
        ceilValue >>= 1;
        while (ceilValue != 0)
        {
            ceilValue >>= 1;
            ++exponent;
        }
        if (exponent > 0)
        {
            storeMaxExponent(primeFactors, Long.valueOf(2), exponent);
        }
    }

    /**
     * Factorizes the specified set of <code>numbers</code> and stores the calculated base/exponent entries in
     * the <code>primeFactors</code> map.
     *
     * @param numbers Set of numbers to factorize
     * @param primeFactors Map of calculated base / exponent entries
     */
    public static void factorize(@Nonnull final Set<Long> numbers, @Nonnull final Map<Long, Integer> primeFactors)
        throws InterruptedException
    {
        Set<Long> notPowerTwo = new TreeSet<>();
        fastPower2Factorize(numbers, notPowerTwo, primeFactors);
        for (Long nr : notPowerTwo)
        {
            if (nr < 1)
            {
                throw new ArithmeticException("Factorization of 0 and of negative numbers is not implemented");
            }
            baseFactorize(nr, primeFactors);
        }
    }

    /**
     * Factorizes the specified number nd stores the calculated base/exponent entries in
     * the <code>primeFactors</code> map.
     *
     * @param nr Input number to factorize
     * @param primeFactors Map of calculated base / exponent entries
     */
    private static void baseFactorize(final long nr, @Nonnull final Map<Long, Integer> primeFactors)
        throws InterruptedException
    {
        long remainder = nr;
        for (int idx = 0; idx < PRIMES_TO_100.length && remainder > 1; ++idx)
        {
            final long factor = PRIMES_TO_100[idx];
            int exponent = 0;
            while (remainder % factor == 0 && remainder > 1)
            {
                ++exponent;
                remainder /= factor;
            }
            if (exponent > 0)
            {
                storeMaxExponent(primeFactors, factor, exponent);
            }
        }

        if (remainder > 1)
        {
            final long maxStoredPrime = PRIMES_TO_100[PRIMES_TO_100.length - 1];
            if (remainder <= (maxStoredPrime * maxStoredPrime))
            {
                primeFactors.putIfAbsent(remainder, Integer.valueOf(1));
            }
            else
            {
                extendedSimpleFactorize(remainder, primeFactors);
            }
        }
    }

    /**
     * Continues factorization of a number that comprises prime factors larger than 100.
     * This method is called by baseFactorize whenever required.
     *
     * @param nr Input number to factorize
     * @param primeFactors Map of calculated base / exponent entries
     */
    private static void extendedSimpleFactorize(final long nr, @Nonnull final Map<Long, Integer> primeFactors)
        throws InterruptedException
    {
        long remainder = nr;
        long factor = 101;
        long range = longCeilingSqrt(remainder);

        // Primitive factorization method: Try dividing by all odd numbers. Since smaller numbers are probed before
        // greater ones, and all numbers are either prime numbers or the product of smaller prime numbers,
        // only prime numbers produce a zero result in the modulo divion of the remainder value, because the prime
        // factors of non-prime numbers were already found and extracted by previous iterations.
        int intrCheckCtr = 0;
        while (remainder > 1 && factor <= range)
        {
            int exponent = 0;
            while (remainder % factor == 0)
            {
                ++exponent;
                remainder /= factor;
            }
            if (exponent > 0)
            {
                range = longCeilingSqrt(remainder);
                storeMaxExponent(primeFactors, factor, exponent);
            }
            factor += 2;
            ++intrCheckCtr;
            if (intrCheckCtr >= INTR_CHECK_FREQ)
            {
                if (Thread.interrupted())
                {
                    throw new InterruptedException("Prime factorization thread was interrupted");
                }
            }
        }

        if (remainder > 1)
        {
            storeMaxExponent(primeFactors, remainder, 1);
        }
    }

    /**
     * Calculates the least common multiple (LCM) of the specified set of numbers.
     *
     * @param numbers Set of numbers for LCM calculation
     * @return Least common multiple of the specified set of numbers
     */
    public static Long leastCommonMultiple(@Nonnull final Set<Long> numbers)
        throws InterruptedException
    {
        Long result = Long.valueOf(0);
        final int count = numbers.size();
        if (count > 1)
        {
            Map<Long, Integer> primeFactors = new TreeMap<>();
            Set<Long> notPowerTwo = new TreeSet<>();

            fastPower2Factorize(numbers, notPowerTwo, primeFactors);

            final Iterator<Long> nrIter = notPowerTwo.iterator();
            while (nrIter.hasNext())
            {
                final long nr = nrIter.next();
                baseFactorize(nr, primeFactors);
            }

            result = productOfFactors(primeFactors);
        }
        else
        if (count == 1)
        {
            final Iterator<Long> numbersIter = numbers.iterator();
            result = numbersIter.next();
        }
        return result;
    }

    /**
     * Calculates the product of the specified map of base values and their exponents.
     *
     * @param primeFactors Map of base / exponent entries
     * @return Product of all <code>base</code> to the power of <code>exponent</code> values
     */
    private static long productOfFactors(@Nonnull final Map<Long, Integer> primeFactors)
    {
        long result = 0;
        Iterator<Entry<Long, Integer>> factorsIter = primeFactors.entrySet().iterator();
        if (factorsIter.hasNext())
        {
            long product = 1;
            do
            {
                final Entry<Long, Integer> factorsEntry = factorsIter.next();
                final long factor = factorsEntry.getKey();
                final long exponent = factorsEntry.getValue();
                for (int cycle = 0; cycle < exponent; ++cycle)
                {
                    if (Long.MAX_VALUE / factor >= product)
                    {
                        product *= factor;
                    }
                    else
                    {
                        throw new ArithmeticException("Product of prime factors is out of range");
                    }
                }
            }
            while (factorsIter.hasNext());
            result = product;
        }
        return result;
    }

    /**
     * Stores the greatest <code>exponent</code> for the base value <code>factor</code> as a base / exponent entry in
     * the <code>primeFactors</code> map.
     * <br/><br/>
     * If no base / exponent entry for the specified <code>base</code> exists in the <code>primeFactors</code> map,
     * or if the specified <code>exponent</code> is greater than the currently stored exponent of an existing
     * base / exponent entry for the same <code>base</code>, the specified <code>base</code> / <code>exponent</code>
     * entry is stored in the <code>primeFactors</code> map.
     *
     * @param primeFactors Map of base / exponent entries
     * @param base Base value for the map entry
     * @param exponent Exponent value for the map entry
     */
    private static void storeMaxExponent(
        @Nonnull final Map<Long, Integer>   primeFactors,
        @Nonnull final Long                 base,
        @Nonnull final Integer              exponent
    )
    {
        final Integer storedExponent = primeFactors.get(base);
        if (storedExponent == null || storedExponent < exponent)
        {
            primeFactors.put(base, exponent);
        }
    }

    /**
     * Returns the specified <code>value</code> if it is in the range
     * [<code>lowerBound</code>, <code>upperBound</code>], otherwise, if <code>value</code> is less than
     * <code>lowerBound</code>, returns <code>lowerBound</code>, and if <code>value</code> is greater than
     * <code>upperBound</code>, returns <code>upperBound</code>.
     *
     * @param <T> Type of the input parameters
     * @param lowerBound The minimum value that may be returned
     * @param value The value to check for being contained in the range [lowerBound, upperBound]
     * @param upperBound The maximum value that may be returned
     * @return value in range [<code>lowerBound</code>, <code>upperBound</code>]
     */
    public static <T extends Comparable<T>> T bounds(
        @Nonnull final T    lowerBound,
        @Nonnull final T    value,
        @Nonnull final T    upperBound
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
