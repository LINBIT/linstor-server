package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.ValueOutOfRangeException;

/**
 * DRBD volume number
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeNumber implements Comparable<VolumeNumber>
{
    public static final int VOLUME_NR_MIN = 0;

    // Valid volume numbers in DRBD are in the range [0, 65534]
    // 65535 is a DRBD protocol reserved value
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int VOLUME_NR_MAX = (1 << 16) - 2;

    private static final String VOLUME_NR_EXC_FORMAT =
        "Volume number %d is out of range [%d - %d]";

    public final int value;

    public VolumeNumber(int number) throws ValueOutOfRangeException
    {
        volumeNrCheck(number);
        value = number;
    }

    public int getValue()
    {
        return value;
    }

    @Override
    public int compareTo(VolumeNumber other)
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

    @Override
    public boolean equals(Object other)
    {
        return other != null &&
            (other instanceof VolumeNumber) &&
            ((VolumeNumber) other).value == this.value;
    }

    @Override
    public int hashCode()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Integer.toString(value);
    }

    /**
     * Checks the validity of a DRBD volume number
     *
     * @param volNr The volume number to check
     * @throws ValueOutOfRangeException If the volume number is out of range
     */
    public static void volumeNrCheck(int volNr) throws ValueOutOfRangeException
    {
        Checks.genericRangeCheck(volNr, VOLUME_NR_MIN, VOLUME_NR_MAX, VOLUME_NR_EXC_FORMAT);
    }
}
