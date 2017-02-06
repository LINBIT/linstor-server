package com.linbit.drbdmanage;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.NumberAlloc;
import com.linbit.ValueOutOfRangeException;

/**
 * Allocator for finding unoccupied Unix/Posix special file minor numbers
 *
 * @author Rene Blauensteiner &lt;rene.blauensteiner@linbit.com&gt;
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MinorNumberAllocator
{
    /**
     * Retrieves a free (unused) minor number
     *
     * @param occupied List of unique occupied minor numbers sorted in ascending order
     * @param minorOffset The start value for finding free minor numbers. The method will
     *     attempt to find an unoccupied number equal or greater to minorOffset first, and
     *     only if no such number can be found, it will attempt to find an unoccupied minor
     *     number less than minorOffset (all within the specified range)
     * @param minMinorNr Lower bound of the minor number range
     * @param maxMinorNr Upper bound of the minor number range
     * @return Free (unoccupied) minor number within the specified range
     * @throws ExhaustedPoolException If all minor numbers within the specified range are occupied
     */
    public MinorNumber getFreeMinorNumber(
        int[] occupied,
        MinorNumber minorOffset,
        MinorNumber minMinorNr,
        MinorNumber maxMinorNr
    )
        throws ExhaustedPoolException
    {
        if (minMinorNr.value > maxMinorNr.value ||
            minorOffset.value < minMinorNr.value || minorOffset.value > maxMinorNr.value)
        {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid input values: minMinorNr(%d) maxMinorNr(%d), minorOffset(%d)",
                    minMinorNr, maxMinorNr, minorOffset
                )
            );
        }

        int minorNr = minorOffset.value;
        if (occupied.length > 0)
        {
            int chkIdx = NumberAlloc.findInsertIndex(occupied, minorNr);
            if (chkIdx >= 0)
            {
                // Number is in use, recycle a free minor number
                //
                // Try finding a free number in the range of numbers
                // greater than the current minor number offset
                try
                {
                    minorNr = NumberAlloc.getFreeNumber(occupied, minorOffset.value, maxMinorNr.value);
                }
                catch (ExhaustedPoolException poolExc)
                {
                    // No free numbers in the high range, try finding a free number
                    // in the range of numbers less than the current minor number
                    // If there are no free numbers in the high range either,
                    // an ExhaustedPoolException is thrown at this point
                    minorNr = NumberAlloc.getFreeNumber(occupied, minMinorNr.value, minorOffset.value);
                }
            }
        }
        MinorNumber result;
        try
        {
            result = new MinorNumber(minorNr);
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid minor number",
                valueExc
            );
        }
        return result;
    }
}
