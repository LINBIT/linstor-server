package com.linbit.drbdmanage;

import com.linbit.ValueOutOfRangeException;

/**
 * Unix minor number
 *
 * @author raltnoeder
 */
public class MinorNumber
{
    public static final int MINOR_NR_MIN = 0;
    public static final int MINOR_NR_MAX = (1 << 20) - 1;

    private static final String MINOR_NR_EXC_FORMAT =
        "Minor number %d is out of range [%d - %d]";

    public final int value;

    public MinorNumber(int number) throws ValueOutOfRangeException
    {
        minorNrCheck(number);
        value = number;
    }

    /**
     * Checks the validity of a UNIX minor number
     *
     * @param minorNr The minor number to check
     * @throws ValueOutOfRangeException If the minor number is out of range
     */
    public static void minorNrCheck(int minorNr) throws ValueOutOfRangeException
    {
        Checks.genericRangeCheck(minorNr, MINOR_NR_MIN, MINOR_NR_MAX, MINOR_NR_EXC_FORMAT);
    }
}
