package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
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

    // Properties container for this volume
    private Props volumeProps;

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, resourceRef.getObjProt(), volumeProps);
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
