package com.linbit.utils;

import java.util.Arrays;
import java.util.Collection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
public class AlignTest
{
    @Test
    @Parameters(method = "generateParamsValid")
    public void testValid(AlignConfiguration data)
    {
        Align test = new Align(data.base);
        assertEquals(data.floor, test.floor(data.value));
        assertEquals(data.ceiling, test.ceiling(data.value));

    }

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "generateParamsInvalidBasis")
    public void testInvalidBasis(Long base)
    {
        Align test = new Align(base);
    }

    @Test(expected = ArithmeticException.class)
    @Parameters(method = "generateParamsInvalidArguments")
    public void testInvalidArguments(AlignConfiguration data)
    {
        Align test = new Align(data.base);
        test.floor(data.value);
        test.ceiling(data.value);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private Collection<AlignConfiguration> generateParamsValid()
    {
        AlignConfiguration[] data = new AlignConfiguration[]
        {
            new AlignConfiguration(10, 5, 0, 10),
            new AlignConfiguration(10, 0, 0, 0),
            new AlignConfiguration(10, 12354, 12350, 12360),
            new AlignConfiguration(1000, 7, 0, 1000),
            new AlignConfiguration(1000, 854870, 854000, 855000),
            new AlignConfiguration(Long.MAX_VALUE, 1325437654, 0, Long.MAX_VALUE),
            new AlignConfiguration(10, Long.MAX_VALUE - 10, Long.MAX_VALUE - 17, Long.MAX_VALUE - 7),
            new AlignConfiguration(7, 5, 0, 7),
            new AlignConfiguration(7, 54563, 54558, 54565),
            new AlignConfiguration(3, 4, 3, 6),
            new AlignConfiguration(3, 95367, 95367, 95367),
            new AlignConfiguration(1234, 98, 0, 1234),
            new AlignConfiguration(1234, 987654321, 987654112, 987655346),
            new AlignConfiguration(
                Long.MAX_VALUE / 2,
                (Long.MAX_VALUE / 2) + 60,
                Long.MAX_VALUE / 2,
                Long.MAX_VALUE - 1
            )
        };
        return Arrays.asList(data);
    }

    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private Collection<Long> generateParamsInvalidBasis()
    {
        Long[] data = new Long[]
        {
            new Long(0), new Long(-1), new Long(-543245), Long.MIN_VALUE
        };
        return Arrays.asList(data);
    }

    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
    private Collection<AlignConfiguration> generateParamsInvalidArguments()
    {
        AlignConfiguration[] data = new AlignConfiguration[]
        {
            new AlignConfiguration(10, -3, 0, 0),
            new AlignConfiguration(Long.MAX_VALUE / 2, Long.MAX_VALUE, 0, 0),
            new AlignConfiguration(10, Long.MAX_VALUE, 0, 0)
        };
        return Arrays.asList(data);
    }

    private static class AlignConfiguration
    {
        public final long base;
        public final long value;
        public final long floor;
        public final long ceiling;

        public AlignConfiguration(long baseRef, long valueRef, long floorRef, long ceilingRef)
        {
            base = baseRef;
            value = valueRef;
            floor = floorRef;
            ceiling = ceilingRef;
        }
    }
}
