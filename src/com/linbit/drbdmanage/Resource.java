package com.linbit.drbdmanage;

import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource
{
    public UUID getUuid();

    public ResourceDefinition getDefinition();

    public Volume getVolume(VolumeNumber volNr);

    public Node getAssignedNode();
}
