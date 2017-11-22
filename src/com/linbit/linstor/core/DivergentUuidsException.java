package com.linbit.linstor.core;

import java.util.UUID;

public class DivergentUuidsException extends DivergentDataException
{
    private static final long serialVersionUID = 7663024682081404303L;

    public DivergentUuidsException(String message)
    {
        super(message);
    }

    public DivergentUuidsException(
        String clazzName,
        String localId,
        String remoteId,
        UUID localUuid,
        UUID remoteUuid
    )
    {
        this(
            String.format(
                "%s UUID mismatch. Received '%s' from controller, but had locally '%s'. Id from local: '%s', " +
                    "Id from remote: '%s'. " +
                    "Dropping connection and resyncing with controller",
                clazzName,
                remoteUuid.toString(),
                localUuid.toString(),
                localId,
                remoteId
            )
        );
    }
}
