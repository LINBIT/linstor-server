package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeDefinitionData implements VolumeDefinition
{
    // Object identifier
    private UUID objId;

    // Resource definition this VolumeDefinition belongs to
    private ResourceDefinition resourceDfn;

    // DRBD volume number
    private VolumeNumber volumeNr;

    // DRBD device minor number
    private MinorNumber minorNr;

    // Net volume size in kiB
    private long volumeSize;

    VolumeDefinitionData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        MinorNumber minor,
        long volSize
    )
        throws MdException, AccessDeniedException
    {
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, VolumeNumber.class, volNr);
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, MinorNumber.class, minor);

        // Creating a new volume definition requires CHANGE access to the resource definition
        resDfnRef.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        try
        {
            Checks.genericRangeCheck(
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB,
                "Volume size value %d is out of range [%d - %d]"
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            String excMessage = String.format(
                "Volume size value %d is out of range [%d - %d]",
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB
            );
            if (valueExc.getViolationType() == ValueOutOfRangeException.ViolationType.TOO_LOW)
            {
                throw new MinSizeException(excMessage);
            }
            else
            {
                throw new MaxSizeException(excMessage);
            }
        }

        objId = UUID.randomUUID();
        resourceDfn = resDfnRef;
        volumeNr = volNr;
        minorNr = minor;
        volumeSize = volSize;
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceDefinition getResourceDfn()
    {
        return resourceDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber(AccessContext accCtx)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeNr;
    }

    @Override
    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return minorNr;
    }

    @Override
    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        minorNr = newMinorNr;
    }

    @Override
    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize;
    }

    @Override
    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumeSize = newVolumeSize;
    }
}
