package com.linbit.drbdmanage;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.TransactionSimpleObject;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeDefinitionData extends BaseTransactionObject implements VolumeDefinition
{
    // Object identifier
    private final UUID objId;

    // Resource definition this VolumeDefinition belongs to
    private ResourceDefinition resourceDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    // DRBD device minor number
    private TransactionSimpleObject<MinorNumber> minorNr;

    // Net volume size in kiB
    private TransactionSimpleObject<Long> volumeSize;

    // Properties container for this volume definition
    private Props vlmDfnProps;

    // State flags
    private StateFlags<VlmDfnFlags> flags;

    private VolumeDefinitionDataDatabaseDriver dbDriver;

    VolumeDefinitionData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        MinorNumber minor,
        long volSize,
        SerialGenerator srlGen
    )
        throws MdException, AccessDeniedException, SQLException
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

        dbDriver = DrbdManage.getVolumeDefinitionDataDatabaseDriver(this);

        volumeNr = volNr;
        minorNr = new TransactionSimpleObject<MinorNumber>(
            minor,
            dbDriver.getMinorNumberDriver()
        );
        volumeSize = new TransactionSimpleObject<Long>(
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        // TODO: do not create new prop, but load existing container
        vlmDfnProps = SerialPropsContainer.createRootContainer(srlGen);

        flags = new VlmDfnFlagsImpl(
            resDfnRef.getObjProt(),
            dbDriver.getStateFlagsPersistence()
        );

        transObjs = Arrays.asList(
            vlmDfnProps,
            resourceDfn,
            minorNr,
            volumeSize,
            flags
        );
    }

     // TODO: gh - implement static VolumeDefinitionData.getInstance(...)

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), vlmDfnProps);
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
        return minorNr.get();
    }

    @Override
    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        minorNr.set(newMinorNr);
    }

    @Override
    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    @Override
    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumeSize.set(newVolumeSize);
    }

    @Override
    public StateFlags<VlmDfnFlags> getFlags()
    {
        return flags;
    }

    private static final class VlmDfnFlagsImpl extends StateFlagsBits<VlmDfnFlags>
    {
        VlmDfnFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(VlmDfnFlags.ALL_FLAGS), persistenceRef);
        }
    }
}
