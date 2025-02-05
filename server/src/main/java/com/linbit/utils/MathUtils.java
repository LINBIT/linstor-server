package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class MathUtils
{
    private static final int[] PRIMES_TO_100 =
    {
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41,
        43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97
    };

    // Frequency for interrupt checks in interruptible methods, every INTR_CHECK_FREQ cycles
    private static final int INTR_CHECK_FREQ    = 10000;

    // The maximum value that will not overflow a signed 64 bit field if squared
    private static final long LONG_SQRT_MAX     = 0xB504F333L;

    // The maximum value that is a power of two and that can be represented by the long datatype
    private static final long LONG_POWER2_MAX   = (1L << 62);

    // Selector that returns true if the value to probe for selection is less than the other value to compare with
    private static final Selector<Long> minSelector =
        (value, other) ->
        {
            return value < other;
        };

    // Selector that returns true if the value to probe for selection is greater than the other value to compare with
    private static final Selector<Long> maxSelector =
        (value, other) ->
        {
            return value > other;
        };

    /**
     * Returns the smallest power of 2 greater than or equal to {@code num}.
     *
     * A few examples:
     *
     * <table border="1px"><tr><thead>Input</thead><thead>Output</thead></tr>
     * <tr><td>1</td><td>1</td></tr>
     * <tr><td>2</td><td>2</td></tr>
     * <tr><td>3</td><td>4</td></tr>
     * <tr><td>4</td><td>4</td></tr>
     * <tr><td>5</td><td>8</td></tr>
     * <tr><td>42</td><td>64</td></tr>
     * <tr><td>9001</td><td>16384</td></tr>
     * </table>
     *
     * @param num Input value; must be greater than or equal to 1 and less than or equal to 2 to the power of 62
     *
     * @throws ArithmeticException If {@code num} is less than 1 or greater than 2 to the power of 62
     */
    public static long longCeilingPowerTwo(final long num) throws ArithmeticException
    {
        if (num < 1)
        {
            throw new ArithmeticException(
                "Calculation of power of 2 floor value for zero and for negative numbers is not supported"
            );
        }
        if (num > LONG_POWER2_MAX)
        {
            throw new ArithmeticException(
                "Cannot calculate power of 2 greater than " + LONG_POWER2_MAX + ": Value is out of range"
            );
        }
        long result = 0;
        int shift = 0;
        while (result < num)
        {
            result = 1L;
            result <<= shift;
            ++shift;
        }
        return result;
    }

    /**
     * Returns the largest power of 2 less than or equal to {@code num}.
     *
     * A few examples:
     *
     * <table border="1px"><tr><thead>Input</thead><thead>Output</thead></tr>
     * <tr><td>1</td><td>1</td></tr>
     * <tr><td>2</td><td>2</td></tr>
     * <tr><td>3</td><td>2</td></tr>
     * <tr><td>4</td><td>4</td></tr>
     * <tr><td>5</td><td>4</td></tr>
     * <tr><td>42</td><td>32</td></tr>
     * <tr><td>9001</td><td>8192</td></tr>
     * </table>
     *
     * @param num Input value; must be greater than or equal to 1
     *
     * @throws ArithmeticException If {@code num} is less than 1
     */
    public static long longFloorPowerTwo(final long num)
    {
        if (num < 1)
        {
            throw new ArithmeticException(
                "Calculation of power of 2 floor values for zero and for negative numbers is not supported"
            );
        }

        // Invariant: num >= 1
        long shift = 0;
        long result = num;
        while (result > 1)
        {
            ++shift;
            result >>= 1;
        }
        result <<= shift;
        return result;
    }

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
                if (probe <= LONG_SQRT_MAX)
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
            if (base == 0 || Long.MAX_VALUE / base >= product)
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
     * Calculates and stores the prime factor (always 2 in this case) and exponent of the smallest power of 2 number
     * in the specified set of numbers.
     * <br/><br/>
     * From all positive powers of 2 in the specified set of numbers, this method will find the smallest number and
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
    public static void fastPower2MinFactorize(
        final Set<Long> numbers,
        @Nullable final Set<Long>           remaining,
        final Map<Long, Integer> primeFactors
    )
    {
        fastPower2GenericFactorize(numbers, remaining, primeFactors, minSelector);
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
    public static void fastPower2MaxFactorize(
        final Set<Long> numbers,
        @Nullable final Set<Long>           remaining,
        final Map<Long, Integer> primeFactors
    )
    {
        fastPower2GenericFactorize(numbers, remaining, primeFactors, maxSelector);
    }

    /**
     * Depending on the Selector <code>valueSelector</code>, selects the smallest or greatest power of 2 number, and
     * then calculates and stores the prime factor (always 2 in this case) and exponent in the specified
     * set of numbers.
     * <br/><br/>
     * The prime factor and exponent is stored in the <code>primeFactors</code> map.<br/>
     * If <code>remaining</code> is non-null, all numbers that are not positive powers of 2 are stored in the
     * <code>remaining</code> set.<br/>
     * <br/>
     * The number 1 is not considered to be a power of 2.
     *
     * @param numbers Set of input numbers
     * @param remaining If non-null, receives the set of numbers that are not positive powers of 2
     * @param primeFactors Map of calculated base / exponent entries
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     */
    private static void fastPower2GenericFactorize(
        final Set<Long> numbers,
        @Nullable final Set<Long>           remaining,
        final Map<Long, Integer> primeFactors,
        Selector<Long>                      valueSelector
    )
    {
        long selectedValue = 0;
        for (Long value : numbers)
        {
            // All positive powers of two, and only powers of two, are positive numbers with a hamming weight of 1
            if (value > 1 && Long.bitCount(value) == 1)
            {
                if (valueSelector.select(value, selectedValue))
                {
                    selectedValue = value;
                }
            }
            else
            if (remaining != null)
            {
                remaining.add(value);
            }
        }

        uncheckedFastPower2GenericFactorize(selectedValue, primeFactors, valueSelector);
    }

    /**
     * If <code>number</code> is a power of 2, calculates and stores its prime factor (always 2 in this case) and
     * exponent and stores them in the <code>primeFactors</code> map if there is no entry for base 2 in the map,
     * or if the exponent of an existing entry is greater than the calculated exponent.
     * <br/><br/>
     * If <code>number</code> is not a power of 2, this method does nothing and returns false.
     * <br/><br/>
     * An entry for base 2 and the calculated exponent is stored in the <code>primeFactors</code> map.<br/>
     * <br/>
     * Returns true if <code>number</code> is a power of 2.
     * The number 1 is not considered to be a power of 2.
     *
     * @param number Input number
     * @param primeFactors Map of calculated base / exponent entries
     * @return true if <code>number</code> is a power of 2
     */
    public boolean fastPower2MinFactorize(
        final long number,
        final Map<Long, Integer> primeFactors
    )
    {
        return fastPower2GenericFactorize(number, primeFactors, minSelector);
    }

    /**
     * If <code>number</code> is a power of 2, calculates and stores its prime factor (always 2 in this case) and
     * exponent and stores them in the <code>primeFactors</code> map if there is no entry for base 2 in the map,
     * or if the exponent of an existing entry is smaller than the calculated exponent.
     * <br/><br/>
     * If <code>number</code> is not a power of 2, this method does nothing and returns false.
     * <br/><br/>
     * An entry for base 2 and the calculated exponent is stored in the <code>primeFactors</code> map.<br/>
     * <br/>
     * Returns true if <code>number</code> is a power of 2.
     * The number 1 is not considered to be a power of 2.
     *
     * @param number Input number
     * @param primeFactors Map of calculated base / exponent entries
     * @return true if <code>number</code> is a power of 2
     */
    public boolean fastPower2MaxFactorize(
        final long number,
        final Map<Long, Integer> primeFactors
    )
    {
        return fastPower2GenericFactorize(number, primeFactors, maxSelector);
    }

    /**
     * Generic implementation of fastPower2MinFactorize and fastPower2MaxFactorize. See description of those methods.
     *
     * @param number Input number
     * @param primeFactors Map of calculated base / exponent entries
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     * @return true if <code>number</code> is a power of 2
     */
    private static boolean fastPower2GenericFactorize(
        final long number,
        final Map<Long, Integer> primeFactors,
        Selector<Long> valueSelector
    )
    {
        final boolean isPower2 = number > 1 && Long.bitCount(number) == 1;
        if (isPower2)
        {
            uncheckedFastPower2GenericFactorize(number, primeFactors, valueSelector);
        }
        return isPower2;
    }

    /**
     * Unchecked implementation of min/max factorization of numbers that are powers of 2.
     * <br/><br/>
     * Specified number must be a power of 2. See description of methods fastPower2MinFactorize
     * and fastPower2MaxFactorize.
     *
     * @param number Input number
     * @param primeFactors Map of calculated base / exponent entries
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     */
    private static void uncheckedFastPower2GenericFactorize(
        final long number,
        final Map<Long, Integer> primeFactors,
        Selector<Long> valueSelector
    )
    {
        long remainder = number;
        int exponent = 0;
        remainder >>= 1;
        while (remainder != 0)
        {
            remainder >>= 1;
            ++exponent;
        }
        if (exponent > 0)
        {
            storeSelectedExponent(primeFactors, 2L, exponent, valueSelector);
        }
    }

    /**
     * Factorizes the specified <code>numbers</code> and stores the calculated base/exponent entries in
     * the <code>primeFactors</code> map.
     *
     * Each number in the <code>numbers</code> set must be greater than 0. Attempting to factorize 0 or
     * a negative number generates an <code>ArithmeticException</code>.
     *
     * @param numbers Set of input numbers
     * @param primeFactors Map of calculated base / exponent entries
     */
    public static void factorize(
        final Set<Long> numbers,
        final Map<Long, Integer> primeFactors
    )
        throws InterruptedException
    {
        Set<Long> notPowerTwo = new TreeSet<>();
        fastPower2GenericFactorize(numbers, notPowerTwo, primeFactors, maxSelector);
        for (Long nr : notPowerTwo)
        {
            factorize(nr, primeFactors);
        }
    }

    /**
     * Factorizes the specified <code>number</code> and stores the calculated base/exponent entries in
     * the <code>primeFactors</code> map.
     *
     * The specified <code>number</code> must be greater than 0. Attempting to factorize 0 or
     * a negative number generates an <code>ArithmeticException</code>.
     *
     * @param number Input number
     * @param primeFactors Map of calculated base / exponent entries
     */
    public static void factorize(
        final long number,
        final Map<Long, Integer> primeFactors
    )
        throws InterruptedException
    {
        baseFactorize(number, primeFactors);
    }

    /**
     * Factorizes the specified number and stores the calculated base/exponent entries in
     * the <code>primeFactors</code> map.
     *
     * @param nr Input number to factorize
     * @param primeFactors Map of calculated base / exponent entries
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     */
    private static void baseFactorize(
        final long nr,
        final Map<Long, Integer> primeFactors
    )
        throws InterruptedException
    {
        if (nr < 0)
        {
            throw new ArithmeticException("Factorization of negative numbers is not implemented");
        }

        if (nr > 0)
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
                    storeSelectedExponent(primeFactors, factor, exponent, maxSelector);
                }
            }

            if (remainder > 1)
            {
                final long maxStoredPrime = PRIMES_TO_100[PRIMES_TO_100.length - 1];
                if (remainder <= (maxStoredPrime * maxStoredPrime))
                {
                    storeSelectedExponent(primeFactors, remainder, 1, maxSelector);
                }
                else
                {
                    extendedSimpleFactorize(remainder, primeFactors);
                }
            }
        }
    }

    /**
     * Continues factorization of a number that comprises prime factors larger than 100.
     * This method is called by baseFactorize whenever required.
     *
     * @param nr Input number to factorize
     * @param primeFactors Map of calculated base / exponent entries
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     */
    private static void extendedSimpleFactorize(
        final long nr,
        final Map<Long, Integer> primeFactors
    )
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
                storeSelectedExponent(primeFactors, factor, exponent, maxSelector);
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
            storeSelectedExponent(primeFactors, remainder, 1, maxSelector);
        }
    }

    /**
     * Calculates the least common multiple (LCM) of the specified set of numbers.
     *
     * If the input set of numbers contains more than one entry, all numbers must be greater than 0.
     *
     * @param numbers Set of numbers for LCM calculation
     * @return Least common multiple of the specified set of numbers
     */
    public static Long leastCommonMultiple(final Set<Long> numbers)
        throws InterruptedException
    {
        Long result = 0L;
        final int count = numbers.size();
        if (count > 1)
        {
            Map<Long, Integer> primeFactors = new TreeMap<>();
            Set<Long> notPowerTwo = new TreeSet<>();

            fastPower2MaxFactorize(numbers, notPowerTwo, primeFactors);

            final Iterator<Long> nrIter = notPowerTwo.iterator();
            while (nrIter.hasNext())
            {
                final long nr = nrIter.next();
                if (nr == 0)
                {
                    throw new ArithmeticException(
                        "Set of numbers for least common multiple calculation contains zero"
                    );
                }
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
     * Calculates the greatest common divisor (GCD) of the specified set of numbers.
     *
     * If there are no input numbers, or the only input number is zero, then the result is <code>Long.MAX_VALUE</code>.
     *
     * @param numbers Set of numbers for GCD calculation
     * @return Least common multiple of the specified set of numbers
     */
    public static Long greatestCommonDivisor(final Set<Long> numbers)
        throws InterruptedException
    {
        Long result = Long.MAX_VALUE;
        final int count = numbers.size();
        if (count > 1)
        {
            Map<Long, Integer> primeFactors = new TreeMap<>();
            Set<Long> notPowerTwo = new TreeSet<>();

            fastPower2MinFactorize(numbers, notPowerTwo, primeFactors);

            final Iterator<Long> nrIter = notPowerTwo.iterator();
            if (primeFactors.size() == 0)
            { // None of the numbers were powers of 2, initialize primeFactors
                Long initNumber = 0L;
                while (initNumber == 0L && nrIter.hasNext())
                {
                   initNumber = nrIter.next();
                }
                if (initNumber != 0)
                {
                    factorize(initNumber, primeFactors);
                }
            }
            while (nrIter.hasNext())
            {
                final Map<Long, Integer> updatedFactors = new TreeMap<>();
                final long nr = nrIter.next();
                long remainder = nr;
                for (final Long factor : primeFactors.keySet())
                {
                    int exponent = 0;
                    while (remainder > 1 && remainder % factor == 0)
                    {
                        remainder /= factor;
                        ++exponent;
                    }
                    updatedFactors.put(factor, exponent);
                }
                for (Map.Entry<Long, Integer> factorEntry : updatedFactors.entrySet())
                {
                    final Long factor = factorEntry.getKey();
                    final Integer exponent = factorEntry.getValue();

                    if (exponent == 0)
                    {
                        primeFactors.remove(factor);
                    }
                    else
                    {
                        storeSelectedExponent(primeFactors, factor, exponent, minSelector);
                    }
                }
            }

            result = productOfFactors(primeFactors);
        }
        else
        if (count == 1)
        {
            final Iterator<Long> numbersIter = numbers.iterator();
            final long nr = numbersIter.next();
            if (nr > 0)
            {
                result = nr;
            }
        }
        return result == 0 ? 1 : result;
    }

    /**
     * Calculates the product of the specified map of base values and their exponents.
     *
     * @param primeFactors Map of base / exponent entries
     * @return Product of all <code>base</code> to the power of <code>exponent</code> values
     */
    private static long productOfFactors(final Map<Long, Integer> primeFactors)
    {
        long result = 0;
        long product = 1;
        {
            final Integer exponent = primeFactors.get(2L);
            if (exponent != null)
            {
                if (exponent < 0 || exponent >= 63)
                {
                    throw new ArithmeticException("Exponent " + exponent + " is out of range");
                }
                product <<= exponent;
            }
        }
        Iterator<Entry<Long, Integer>> factorsIter = primeFactors.entrySet().iterator();
        if (factorsIter.hasNext())
        {
            do
            {
                final Entry<Long, Integer> factorsEntry = factorsIter.next();
                final long factor = factorsEntry.getKey();
                if (factor != 2)
                {
                    final long exponent = factorsEntry.getValue();
                    if (exponent < 0 || exponent >= 63)
                    {
                        throw new ArithmeticException("Exponent " + exponent + " is out of range");
                    }
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
            }
            while (factorsIter.hasNext());
            result = product;
        }
        return result;
    }

    /**
     * Depending on the specified Selector <code>valueSelector</code>, stores the smallest or greatest
     * <code>exponent</code> for the base value <code>factor</code> as a base / exponent entry in
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
     * @param valueSelector Selector that decides whether a number is greater or smaller than another number
     */
    private static void storeSelectedExponent(
        final Map<Long, Integer> primeFactors,
        final Long base,
        final Integer exponent,
        Selector<Long> valueSelector
    )
    {
        final Integer storedExponent = primeFactors.get(base);
        if (storedExponent == null || valueSelector.select(exponent.longValue(), storedExponent.longValue()))
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

    private interface Selector<T>
    {
        boolean select(final T value, final T other);
    }
}
