package com.linbit.drbdmanage;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.NumberAlloc;
import com.linbit.ValueOutOfRangeException;

/**
 * Allocator for finding unoccupied TCP port numbers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpPortNumberAlloc
{
    /**
     * Allocates a free (unused) TCP port number
     *
     * @param occupied List of unique occupied TCP port numbers sorted in ascending order
     * @param minPortNr Lower bound of the port number range
     * @param maxPortNr Upper bound of the port number range
     * @return Free (unoccupied) TCP port number within the specified range
     * @throws ExhaustedPoolException If all TCP port numbers within the specified range are occupied
     */
    public static TcpPortNumber getFreePortNumber(
        int[] occupied,
        TcpPortNumber minPortNr,
        TcpPortNumber maxPortNr
    )
        throws ExhaustedPoolException
    {
        TcpPortNumber result;
        try
        {
            result = new TcpPortNumber(
                NumberAlloc.getFreeNumber(occupied, minPortNr.value, maxPortNr.value)
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                "The algorithm allocated an invalid port number",
                valueExc
            );
        }
        return result;
    }
}
