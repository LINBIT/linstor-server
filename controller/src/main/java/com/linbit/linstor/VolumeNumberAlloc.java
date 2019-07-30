package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.NumberAlloc;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.VolumeNumber;

/**
 * Allocator for unoccupied DRBD volume numbers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeNumberAlloc
{
    /** Allocates a free (unused) volume number. <br/>
     *  <br/>
     *  This method is the same as calling {@link #getFreeVolumeNumber(int[], VolumeNumber, VolumeNumber)}
     *  with {@link VolumeNumber#VOLUME_NR_MIN} and {@link VolumeNumber#VOLUME_NR_MAX} as range limitation.
     *
     * @param occupied
     * @return An unoccupied {@link VolumeNumber}
     * @throws ExhaustedPoolException If all volume numbers within the specified range are occupied
     */
    public static VolumeNumber getFreeVolumeNumber(int[] occupied) throws ExhaustedPoolException
    {
        VolumeNumber result;
        try
        {
            result = new VolumeNumber(
                NumberAlloc.getFreeNumber(
                    occupied,
                    VolumeNumber.VOLUME_NR_MIN,
                    VolumeNumber.VOLUME_NR_MAX
                )
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid volume number",
                valueExc
            );
        }
        return result;
    }

    /**
     * Allocates a free (unused) volume number
     *
     * @param occupied List of unique occupied volume numbers sorted in ascending order
     * @param minVolNr Lower bound of the volume number range
     * @param maxVolNr Upper bound of the volume number range
     * @return An unoccupied {@link VolumeNumber}
     * @throws ExhaustedPoolException If all volume numbers within the specified range are occupied
     */
    public static VolumeNumber getFreeVolumeNumber(
        int[] occupied,
        VolumeNumber minVolNr,
        VolumeNumber maxVolNr
    )
        throws ExhaustedPoolException
    {
        VolumeNumber result;
        try
        {
            result = new VolumeNumber(
                NumberAlloc.getFreeNumber(occupied, minVolNr.value, maxVolNr.value)
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid volume number",
                valueExc
            );
        }
        return result;
    }

    private VolumeNumberAlloc()
    {
    }
}
