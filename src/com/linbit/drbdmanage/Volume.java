package com.linbit.drbdmanage;

import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Volume
{
    public UUID getUuid();

    public Resource getResource();

    public ResourceDefinition getResourceDfn();

    public VolumeDefinition getVolumeDfn();
}
