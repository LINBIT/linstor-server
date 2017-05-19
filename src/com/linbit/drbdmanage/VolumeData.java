package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import java.util.UUID;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;

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

    // State flags
    private StateFlags<VlmFlags> flags;

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

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private static final class VlmFlagsImpl extends StateFlagsBits<VlmFlags>
    {
        VlmFlagsImpl(ObjectProtection objProtRef)
        {
            super(objProtRef, StateFlagsBits.getMask(VlmFlags.ALL_FLAGS));
        }
    }
}
