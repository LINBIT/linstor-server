package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeDefinitionData extends BaseTransactionObject implements VolumeDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

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

    private final TransactionMap<String, Volume> volumes;

    private final VolumeDefinitionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeDefinitionData, Boolean> deleted;

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
        dbgInstanceId = UUID.randomUUID();
        resourceDfn = resDfnRef;

        dbDriver = LinStor.getVolumeDefinitionDataDatabaseDriver();

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

        volumes = new TransactionMap<>(new TreeMap<String, Volume>(), null);

        flags = new VlmDfnFlagsImpl(
            resDfnRef.getObjProt(),
            this,
            dbDriver.getStateFlagsPersistence(),
            initFlags
        );
        deleted = new TransactionSimpleObject<>(this, false, null);

        transObjs = Arrays.asList(
            vlmDfnProps,
            resourceDfn,
            minorNr,
            volumeSize,
            flags,
            deleted
        );

        ((ResourceDefinitionData) resourceDfn).putVolumeDefinition(accCtx, this);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public static VolumeDefinitionData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        MinorNumber minor,
        Long volSize,
        VlmDfnFlags[] initFlags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, MdException, LinStorDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeDefinitionData volDfnData = null;

        VolumeDefinitionDataDatabaseDriver driver = LinStor.getVolumeDefinitionDataDatabaseDriver();

        volDfnData = driver.load(resDfn, volNr, false, transMgr);

        if (failIfExists && volDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeDefinition already exists");
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
            volDfnData.initialized();
            volDfnData.setConnection(transMgr);
        }
        return volDfnData;
    }

    public static VolumeDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmDfnUuid,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        long vlmSize,
        MinorNumber minorNumber,
        VlmDfnFlags[] flags,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        VolumeDefinitionDataDatabaseDriver driver = LinStor.getVolumeDefinitionDataDatabaseDriver();
        VolumeDefinitionData vlmDfnData;
        try
        {
            vlmDfnData = driver.load(rscDfn, vlmNr, false, transMgr);
            if (vlmDfnData == null)
            {
                vlmDfnData = new VolumeDefinitionData(
                    vlmDfnUuid,
                    accCtx,
                    rscDfn,
                    vlmNr,
                    minorNumber,
                    vlmSize,
                    StateFlagsBits.getMask(flags),
                    transMgr
                );
            }
            vlmDfnData.initialized();
            vlmDfnData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return vlmDfnData;
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
    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
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
    public MinorNumber setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return minorNr.set(newMinorNr);
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
    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return volumeSize.set(newVolumeSize);
    }

    @Override
    public StateFlags<VlmDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void putVolume(AccessContext accCtx, VolumeData volumeData) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.put(getResourceId(volumeData.getResource()), volumeData);
    }

    public void removeVolume(AccessContext accCtx, VolumeData volumeData) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.remove(getResourceId(volumeData.getResource()));
    }

    @Override
    public Iterator<Volume> iterateVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().iterator();
    }

    private String getResourceId(Resource rsc)
    {
        return rsc.getAssignedNode().getName().value + "/" +
            rsc.getDefinition().getName().value;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, VolumeDefinition.VlmDfnFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            ((ResourceDefinitionData) resourceDfn).removeVolumeDefinition(accCtx, this);

            for (Volume vlm : volumes.values())
            {
                vlm.delete(accCtx);
            }

            dbDriver.delete(this, transMgr);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume definition");
        }
    }

    @Override
    public VlmDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new VlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getMinorNr(accCtx).value,
            getVolumeSize(accCtx),
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map()
        );
    }



    @Override
    public String toString()
    {
        return "Rsc: '" + resourceDfn.getName() + "', " +
               "VlmNr: '" + volumeNr + "'";
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
