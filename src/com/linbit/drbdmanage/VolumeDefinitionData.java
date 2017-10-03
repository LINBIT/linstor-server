package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
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
    private final ResourceDefinition resourceDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    // DRBD device minor number
    private final TransactionSimpleObject<VolumeDefinitionData, MinorNumber> minorNr;

    // Net volume size in kiB
    private final TransactionSimpleObject<VolumeDefinitionData, Long> volumeSize;

    // Properties container for this volume definition
    private final Props vlmDfnProps;

    // State flags
    private final StateFlags<VlmDfnFlags> flags;

    private final VolumeDefinitionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private VolumeDefinitionData(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        MinorNumber minor,
        long volSize,
        long initFlags,
        TransactionMgr transMgr
    )
        throws MdException, AccessDeniedException, SQLException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            resDfnRef,
            volNr,
            minor,
            volSize,
            initFlags,
            transMgr
        );
    }

    /*
     * used by database drivers and tests
     */
    VolumeDefinitionData(
        UUID uuid,
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        MinorNumber minor,
        long volSize,
        long initFlags,
        TransactionMgr transMgr
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
            throw new MaxSizeException(excMessage);
        }

        objId = uuid;
        resourceDfn = resDfnRef;

        dbDriver = DrbdManage.getVolumeDefinitionDataDatabaseDriver();

        volumeNr = volNr;
        minorNr = new TransactionSimpleObject<>(
            this,
            minor,
            dbDriver.getMinorNumberDriver()
        );
        volumeSize = new TransactionSimpleObject<>(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        vlmDfnProps = PropsContainer.getInstance(
            PropsContainer.buildPath(resDfnRef.getName(), volumeNr),
            transMgr
        );

        flags = new VlmDfnFlagsImpl(
            resDfnRef.getObjProt(),
            this,
            dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        transObjs = Arrays.asList(
            vlmDfnProps,
            resourceDfn,
            minorNr,
            volumeSize,
            flags
        );
    }

    public static VolumeDefinitionData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        MinorNumber minor,
        long volSize,
        VlmDfnFlags[] initFlags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, MdException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeDefinitionData volDfnData = null;

        VolumeDefinitionDataDatabaseDriver driver = DrbdManage.getVolumeDefinitionDataDatabaseDriver();

        volDfnData = driver.load(resDfn, volNr, false, transMgr);

        if (failIfExists && volDfnData != null)
        {
            throw new DrbdDataAlreadyExistsException("The VolumeDefinition already exists");
        }

        if (volDfnData == null && createIfNotExists)
        {
            volDfnData = new VolumeDefinitionData(
                accCtx,
                resDfn,
                volNr,
                minor,
                volSize,
                StateFlagsBits.getMask(initFlags),
                transMgr
            );
            driver.create(volDfnData, transMgr);
        }

        if (volDfnData != null)
        {
            ((ResourceDefinitionData) resDfn).putVolumeDefinition(accCtx, volDfnData);
            volDfnData.initialized();
        }


        return volDfnData;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), vlmDfnProps);
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeNr;
    }

    @Override
    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return minorNr.get();
    }

    @Override
    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        minorNr.set(newMinorNr);
    }

    @Override
    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    @Override
    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumeSize.set(newVolumeSize);
    }

    @Override
    public StateFlags<VlmDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        ((ResourceDefinitionData) resourceDfn).removeVolumeDefinition(accCtx, this);
        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
    }

    private static final class VlmDfnFlagsImpl extends StateFlagsBits<VolumeDefinitionData, VlmDfnFlags>
    {
        VlmDfnFlagsImpl(
            ObjectProtection objProtRef,
            VolumeDefinitionData parent,
            StateFlagsPersistence<VolumeDefinitionData> persistenceRef,
            long initFlags
        )
        {
            super(
                objProtRef,
                parent,
                StateFlagsBits.getMask(VlmDfnFlags.values()),
                persistenceRef,
                initFlags
            );
        }
    }
}
