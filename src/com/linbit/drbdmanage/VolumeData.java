package com.linbit.drbdmanage;

import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeData implements Volume
{
    // Object identifier
    private UUID objId;

    // Reference to the resource this volume belongs to
    private Resource resourceRef;

    // Reference to the resource definition that defines the resource this volume belongs to
    private ResourceDefinition resourceDfn;

    // Reference to the volume definition that defines this volume
    private VolumeDefinition volumeDfn;

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public Resource getResource()
    {
        return resourceRef;
    }

    @Override
    public ResourceDefinition getResourceDfn()
    {
        return resourceDfn;
    }

    @Override
    public VolumeDefinition getVolumeDfn()
    {
        return volumeDfn;
    }
}
