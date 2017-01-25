package com.linbit.drbdmanage;

import com.linbit.ValueOutOfRangeException;

/**
 * DRBD volume number
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeNumber implements Comparable<VolumeNumber>
{
    public static final int VOLUME_NR_MIN = 0;

    // FIXME: volume numbers were supposed to be unsigned in DRBD 9.0,
    //        but currently, there is at least one signedness bug somewhere
    //        in the network protocol implementation that limits the usable
    //        range to 0 - 32767
    public static final int VOLUME_NR_MAX = (1 << 15) - 1;

    private static final String VOLUME_NR_EXC_FORMAT =
        "Volume number %d is out of range [%d - %d]";

    public final int value;

    public VolumeNumber(int number) throws ValueOutOfRangeException
    {
        volumeNrCheck(number);
        value = number;
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

    public boolean equals(VolumeNumber other)
    {
        return other != null && other.value == this.value;
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
