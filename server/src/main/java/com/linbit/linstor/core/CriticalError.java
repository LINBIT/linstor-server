package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.logging.ErrorReporter;

import java.util.UUID;

public class CriticalError extends Error
{
    public CriticalError(String message)
    {
        super(message);
    }

    /**
     * This will call System.exit(), so make sure no locks are currently held while using this call.
     * @param errorReporter
     * @param message
     */
    public static void die(ErrorReporter errorReporter, CriticalError critErr)
    {
        errorReporter.reportError(new ImplementationError(critErr.getMessage()));
        System.exit(InternalApiConsts.EXIT_CODE_INTERNAL_CRITICAL_ERROR);
    }

    /**
     * Alias for creating a critical error and unwind stack to allow unlock any lockguards
     * @param message
     * @throws CriticalError
     */
    public static void dieSoon(String message) throws CriticalError
    {
        throw new CriticalError(message);
    }

    public static void dieUuidMissmatch(
        String clazzName,
        String localId,
        String remoteId,
        UUID localUuid,
        UUID remoteUuid
    ) throws CriticalError
    {
        throw new CriticalError(
            String.format(
                "%s UUID mismatch. Received '%s' from controller, but had locally '%s'. Id from local: '%s', " +
                    "Id from remote: '%s'. " +
                    "Dropping connection and resyncing with controller",
                clazzName,
                remoteUuid,
                localUuid,
                localId,
                remoteId
            )
        );
    }
}
