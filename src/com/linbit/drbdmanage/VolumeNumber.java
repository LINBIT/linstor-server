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

    // FIXME: VOLUME_NR_MAX is probably something else around ~65530
    //        Check DRBD kernel module for the correct value
    public static final int VOLUME_NR_MAX = (1 << 16) - 2;

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
