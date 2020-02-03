package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.linstor.security.AccessDeniedException;

public class SatelliteDbDriverExceptionHandler
{
    private SatelliteDbDriverExceptionHandler()
    {
    }

    public static void handleAccessDeniedException(AccessDeniedException accDeniedExc)
    {
        throw new ImplementationError(
            "SatelliteDbDriver's accessContext has not enough privileges",
            accDeniedExc
        );
    }
}
