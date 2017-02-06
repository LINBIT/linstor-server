package com.linbit.drbdmanage;

import java.util.UUID;

/**
 * Defines a connection for a DRBD resource
 *
 * @author raltnoeder
 */
public class ConnectionDefinitionData implements ConnectionDefinition
{
    // Object identifier
    private UUID objId;

    @Override
    public UUID getUuid()
    {
        return objId;
    }
}
