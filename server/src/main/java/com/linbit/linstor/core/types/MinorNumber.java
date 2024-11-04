package com.linbit.linstor.core.types;

import com.linbit.Checks;
import com.linbit.ValueOutOfRangeException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Unix minor number
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MinorNumber implements Comparable<MinorNumber>
{
    public static final int MINOR_NR_MIN = 0;
    // MagicNumber exception: shift value
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MINOR_NR_MAX = (1 << 20) - 1;

    private static final String MINOR_NR_EXC_FORMAT =
        "Minor number %d is out of range [%d - %d]";

    public final int value;

    public MinorNumber(int number) throws ValueOutOfRangeException
    {
        minorNrCheck(number);
        value = number;
    }

    @Override
    public int compareTo(MinorNumber other)
    {
        int result = 0;
        if (other == null)
        {
            // null sorts before any existing VolumeNumber
            result = 1;
        }
        else
        {
            if (this.value < other.value)
            {
                result = -1;
            }
            else
            if (this.value > other.value)
            {
                result = 1;
            }
        }
        return result;
    }

    @SuppressFBWarnings("EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS")
    @Override
    public boolean equals(Object other)
    {
        boolean result = false;
        if (this == other)
        {
            result = true;
        }
        else
        {
            try
            {
                if (other != null && value == ((MinorNumber) other).value)
                {
                    result = true;
                }
            }
            catch (ClassCastException castExc)
            {
                if (other instanceof Integer)
                {
                    if (value == (Integer) other)
                    {
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 67 * hash + this.value;
        return hash;
    }

    @Override
    public String toString()
    {
        return Integer.toString(value);
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
