package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.logging.ErrorReporter;

import java.util.UUID;

public class CriticalError
{
    public static final int EXIT_CODE_INTERNAL_CRITICAL_ERROR = 70;

    private CriticalError()
    {
    }

    public static void die(ErrorReporter errorReporter, String message)
    {
        errorReporter.reportError(new ImplementationError(message));
        System.exit(EXIT_CODE_INTERNAL_CRITICAL_ERROR);
    }

    public static void dieUuidMissmatch(
        ErrorReporter errorReporter,
        String clazzName,
        String localId,
        String remoteId,
        UUID localUuid,
        UUID remoteUuid
    )
    {
        die(
            errorReporter,
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
