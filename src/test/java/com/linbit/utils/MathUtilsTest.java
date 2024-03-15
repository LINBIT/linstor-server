package com.linbit.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MathUtilsTest
{
    @Test
    public void testBounds()
    {
        BoundsData[] testInput = new BoundsData[]
        {
            new BoundsData(50, 0, 50, 100),
            new BoundsData(100, 0, Integer.MAX_VALUE, 100),
            new BoundsData(0, 0, Integer.MIN_VALUE, 100),
            new BoundsData(0, 0, -1, 100),
            new BoundsData(100, 0, 101, 100),
            new BoundsData(-5000, -5000, -5000, -1000),
            new BoundsData(-1000, -5000, -1000, -1000),
            new BoundsData(-2180, -5000, -2180, -1000),
            new BoundsData(1300, 1300, 5000, 1300),
            new BoundsData(Integer.MIN_VALUE, Integer.MIN_VALUE, 0, Integer.MIN_VALUE)
        };

        for (BoundsData data : testInput)
        {
            final long result = MathUtils.bounds(data.lowerBound, data.value, data.upperBound);
            assertTrue("Incorrect result " + result + ", expected " + data.result +
                       ", input (" + data.lowerBound + ", " + data.value + ", " + data.upperBound + ")",
                       result == data.result);
        }
    }

    @Test
    public void testFactorize()
        throws InterruptedException
    {
        Map<Long, Integer> solution = new TreeMap<>();
        Map<Long, Integer> primeFactors = new TreeMap<>();
        solution.put(2L, 12);
        solution.put(3L, 3);
        solution.put(5L, 2);
        solution.put(17L, 4);
        solution.put(19L, 1);
        solution.put(23L, 1);
        solution.put(53L, 2);
        solution.put(73L, 3);
        solution.put(911L, 1);

        Set<Long> numbers = new TreeSet<>();
        numbers.add(618365526036480L);
        numbers.add(207942968684800L);

        MathUtils.factorize(numbers, primeFactors);

        assertTrue("Incorrect prime factors count, result count = " + primeFactors.size() +
                   ", expected " + solution.size(),
                   primeFactors.size() == solution.size());

        Iterator<Map.Entry<Long, Integer>> solutionIter = solution.entrySet().iterator();
        Iterator<Map.Entry<Long, Integer>> resultIter = primeFactors.entrySet().iterator();
        while (solutionIter.hasNext())
        {
            Map.Entry<Long, Integer> solutionItem = solutionIter.next();
            Map.Entry<Long, Integer> resultItem = resultIter.next();

            final long solutionFactor = solutionItem.getKey();
            final long solutionExponent = solutionItem.getValue();

            final long resultFactor = resultItem.getKey();
            final long resultExponent = resultItem.getValue();

            assertTrue("Incorrect result factor " + resultFactor + ", expected " + solutionFactor,
                       resultFactor == solutionFactor);
            assertTrue("Incorrect result exponent " + resultExponent + ", expected " + solutionExponent,
                       resultExponent == solutionExponent);
        }

        primeFactors.clear();
        MathUtils.factorize(0, primeFactors);
        if (primeFactors.size() > 0)
        {
            fail("Prime factorization thinks that zero is the product of prime numbers, but mathematicians think " +
                 "this result might be a little bit dubious");
        }

        try
        {
            MathUtils.factorize(-25, primeFactors);
            fail("Prime factorization did not reject a negative input number");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }
    }

    @Test
    public void testFastPower2MinFactorize()
    {
        Set<Long> numbers = new HashSet<>();
        numbers.add(0L);
        numbers.add(1L);
        numbers.add(4L);
        numbers.add(16L);
        numbers.add(24L);
        numbers.add(256L);
        numbers.add(8192L);
        numbers.add(131072L);
        numbers.add(1048576L);

        Set<Long> remaining = new TreeSet<>();
        Map<Long, Integer> primeFactors = new TreeMap<>();

        MathUtils.fastPower2MinFactorize(numbers, remaining, primeFactors);

        for (Map.Entry<Long, Integer> factorEntry : primeFactors.entrySet())
        {
            final long factor = factorEntry.getKey();
            final int exponent = factorEntry.getValue();

            assertTrue("Incorrect prime factor " + factor, factor == 2);
            assertTrue("Incorrect exponent " + exponent, exponent == 2);
        }

        assertTrue("Incorrect count of numbers that are not powers of 2", remaining.size() == 3);
    }

    @Test
    public void testFastPower2MaxFactorize()
    {
        Set<Long> numbers = new HashSet<>();
        numbers.add(0L);
        numbers.add(1L);
        numbers.add(4L);
        numbers.add(16L);
        numbers.add(24L);
        numbers.add(256L);
        numbers.add(8192L);
        numbers.add(131072L);
        numbers.add(1048576L);

        Set<Long> remaining = new TreeSet<>();
        Map<Long, Integer> primeFactors = new TreeMap<>();

        MathUtils.fastPower2MinFactorize(numbers, remaining, primeFactors);

        for (Map.Entry<Long, Integer> factorEntry : primeFactors.entrySet())
        {
            final long factor = factorEntry.getKey();
            final int exponent = factorEntry.getValue();

            assertTrue("Incorrect prime factor " + factor, factor == 2);
            assertTrue("Incorrect exponent " + exponent, exponent == 20);
        }

        assertTrue("Incorrect count of numbers that are not powers of 2", remaining.size() == 3);
    }

    @Test
    public void testFloorSqrt()
    {
        long[] testInput = new long[] {0, 1, 2, 4, 255, 256, 257, (1234567890L * 1234567890L), Long.MAX_VALUE};
        long[] testResult = new long[] {0, 1, 1, 2, 15, 16, 16, 1234567890, 0xB504F333L};

        for (int idx = 0; idx < testInput.length; ++idx)
        {
            final long result = MathUtils.longFloorSqrt(testInput[idx]);
            assertTrue("Result " + result + " does not match expected result " + testResult[idx],
                       result == testResult[idx]);
        }

        try
        {
            MathUtils.longFloorSqrt(-1);
            fail("Negative input number check failed");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }

        try
        {
            MathUtils.longFloorSqrt(Long.MIN_VALUE);
            fail("Negative input number check failed");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }
    }

    @Test
    public void testCeilingSqrt()
    {
        long[] testInput = new long[] {0, 1, 2, 4, 255, 256, 257, (1234567890L * 1234567890L), Long.MAX_VALUE};
        long[] testResult = new long[] {0, 1, 2, 2, 16, 16, 17, 1234567890, 0xB504F334L};

        for (int idx = 0; idx < testInput.length; ++idx)
        {
            final long result = MathUtils.longCeilingSqrt(testInput[idx]);
            assertTrue("Result " + result + " does not match expected result " + testResult[idx],
                       result == testResult[idx]);
        }

        try
        {
            MathUtils.longCeilingSqrt(-1);
            fail("Negative input number check failed");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }

        try
        {
            MathUtils.longCeilingSqrt(Long.MIN_VALUE);
            fail("Negative input number check failed");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }
    }

    @Test
    public void testLeastCommonMultiple()
        throws InterruptedException
    {
        PrimeProductData[] testInput = new PrimeProductData[]
        {
            new PrimeProductData(8192, 4, 8, 256, 512, 4096, 8192),
            new PrimeProductData(4096, 4096),
            new PrimeProductData(8085, 385, 147),
            new PrimeProductData(1260, 60, 63),
            new PrimeProductData(420, 28, 30),
            new PrimeProductData(270, 135, 54),
            new PrimeProductData(34560, 8, 135, 256, 54, 4),
            new PrimeProductData(647721, 911, 711),
            new PrimeProductData(1, 1),
            new PrimeProductData(64, 64),
            new PrimeProductData(510510, 510510),
            new PrimeProductData(510510, 221, 2310),
            new PrimeProductData(0),
            new PrimeProductData(0, 0)
        };

        for (PrimeProductData data : testInput)
        {
            final long result = MathUtils.leastCommonMultiple(data.numbers);
            assertTrue("Result " + result + " does not match expected result " + data.result,
                       result == data.result);
        }

        try
        {
            MathUtils.leastCommonMultiple((new PrimeProductData(0, 8, 135, 256, -1, 54, 4)).numbers);
            fail("LCM calculation failed to reject a negative input number");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }

        try
        {
            MathUtils.leastCommonMultiple((new PrimeProductData(0, 8, 135, 256, 0, 54, 4)).numbers);
            fail("LCM calculation failed to reject a zero input number");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }
    }

    @Test
    public void testGreatestCommonDivisor()
        throws InterruptedException
    {
        PrimeProductData[] testInput = new PrimeProductData[]
        {
            new PrimeProductData(140, 4620, 3920, 3640, 560, 1400, 27300, 3220),
            new PrimeProductData(140, 4620, 3920, 3640, 560, 0, 1400, 27300, 3220),
            new PrimeProductData(1, 400, 735, 77, 209, 299),
            new PrimeProductData(8192, 376832, 466944, 696320, 745472, 991232, 745472),
            new PrimeProductData(1, 1),
            new PrimeProductData(Long.MAX_VALUE, Long.MAX_VALUE),
            new PrimeProductData(Long.MAX_VALUE, 0),
            new PrimeProductData(Long.MAX_VALUE),
        };

        for (PrimeProductData data : testInput)
        {
            final long result = MathUtils.greatestCommonDivisor(data.numbers);
            assertTrue("Result " + result + " does not match expected result " + data.result,
                       result == data.result);
        }
    }

    @Test
    public void testPower()
    {
        PowerData[] testInput = new PowerData[]
        {
            new PowerData(0, 0, 1),
            new PowerData(2, 62, 4611686018427387904L),
            new PowerData(3, 39, 4052555153018976267L),
            new PowerData(5, 27, 7450580596923828125L)
        };

        for (PowerData data : testInput)
        {
            final long result = MathUtils.longPower(data.base, data.exponent);
            assertTrue("Result " + result + " does not match expected result " + data.result,
                       result == data.result);
        }

        try
        {
            MathUtils.longPower(5, -5);
            fail("Negative exponent not detected");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }

        try
        {
            MathUtils.longPower(2, 63);
            fail("Arithmetic overflow not detected");
        }
        catch (ArithmeticException ignored)
        {
            // expected; no-op
        }
    }

    @Test(expected = ArithmeticException.class)
    public void testCeilingPowerTwoFailNegative()
    {
        MathUtils.longCeilingPowerTwo(-1);
    }

    @Test(expected = ArithmeticException.class)
    public void testCeilingPowerTwoFailTypeWidth()
    {
        MathUtils.longCeilingPowerTwo((1L << 62) + 1);
    }

    @Test(expected = ArithmeticException.class)
    public void testCeilingPowerTwoFailZero()
    {
        MathUtils.longCeilingPowerTwo(0);
    }

    @Test(expected = ArithmeticException.class)
    public void testFloorPowerTwoFailZero()
    {
        MathUtils.longFloorPowerTwo(0);
    }

    @Test
    public void testCeilingPowerTwo()
    {
        // 1 -> 1
        assertEquals(1, MathUtils.longCeilingPowerTwo(1));
        // 2 -> 2
        assertEquals(2, MathUtils.longCeilingPowerTwo(2));

        final long max = (Long.MAX_VALUE / 2) + 1;
        for (long powTwo = 4; powTwo < max; powTwo *= 2)
        {
            // loop checks:
            // 3 -> 4
            // 4 -> 4
            // 5 -> 8
            // 7 -> 8
            // 8 -> 8
            // 9 -> 16
            // 15 -> 16
            // 16 -> 16
            // 17 -> 32
            // ...
            assertEquals(powTwo, MathUtils.longCeilingPowerTwo(powTwo - 1));
            assertEquals(powTwo, MathUtils.longCeilingPowerTwo(powTwo));
            assertEquals(powTwo * 2, MathUtils.longCeilingPowerTwo(powTwo + 1));
        }

        assertEquals((1L << 62), MathUtils.longCeilingPowerTwo((1L << 62) - 1));
        assertEquals((1L << 62), MathUtils.longCeilingPowerTwo(1L << 62));
    }

    @Test
    public void testFloorPowerTwo()
    {
        // 1 -> 1
        assertEquals(1, MathUtils.longFloorPowerTwo(1));
        // 2 -> 2
        assertEquals(2, MathUtils.longFloorPowerTwo(2));

        final long max = (Long.MAX_VALUE / 2) + 1;
        for (long powTwo = 4; powTwo < max; powTwo *= 2)
        {
            // loop checks:
            // 3 -> 2
            // 4 -> 4
            // 5 -> 4
            // 7 -> 4
            // 8 -> 8
            // 9 -> 8
            // 15 -> 8
            // 16 -> 16
            // 17 -> 16
            // ...
            assertEquals(powTwo / 2, MathUtils.longFloorPowerTwo(powTwo - 1));
            assertEquals(powTwo, MathUtils.longFloorPowerTwo(powTwo));
            assertEquals(powTwo, MathUtils.longFloorPowerTwo(powTwo + 1));
        }

        assertEquals((1L << 62), MathUtils.longFloorPowerTwo((1L << 62)));
        assertEquals((1L << 62), MathUtils.longFloorPowerTwo((1L << 62) + 1));
        assertEquals((1L << 62), MathUtils.longFloorPowerTwo(Long.MAX_VALUE));
    }

    static class BoundsData
    {
        long lowerBound;
        long value;
        long upperBound;

        long result;

        BoundsData(long initResult, long initLower, long initValue, long initUpper)
        {
            result = initResult;
            lowerBound = initLower;
            value = initValue;
            upperBound = initUpper;
        }
    }

    static class PowerData
    {
        long base;
        int exponent;
        long result;

        PowerData(long initBase, int initExponent, long initResult)
        {
            base = initBase;
            exponent = initExponent;
            result = initResult;
        }
    }

    static class PrimeProductData
    {
        long result;
        Set<Long> numbers;

        PrimeProductData(long initResult, long... initNumbers)
        {
            result = initResult;
            numbers = new HashSet<>();
            for (long nr : initNumbers)
            {
                numbers.add(nr);
            }
        }
    }
}
