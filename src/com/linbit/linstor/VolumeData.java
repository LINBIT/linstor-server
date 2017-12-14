package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
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
public class VolumeData extends BaseTransactionObject implements Volume
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource this volume belongs to
    private final Resource resource;

    // Reference to the resource definition that defines the resource this volume belongs to
    private final ResourceDefinition resourceDfn;

    // Reference to the volume definition that defines this volume
    private final VolumeDefinition volumeDfn;

    private final StorPool storPool;

    // Properties container for this volume
    private final Props volumeProps;

    // State flags
    private final StateFlags<VlmFlags> flags;

    private final TransactionMap<Volume, VolumeConnection> volumeConnections;

    private String blockDevicePath;

    private String metaDiskPath;

    private final VolumeDataDatabaseDriver dbDriver;

    private boolean deleted = false;


    /*
     * used by getInstance
     */
    private VolumeData(
        AccessContext accCtx,
        Resource resRef,
        VolumeDefinition volDfn,
        StorPool storPool,
        String blockDevicePathRef,
        String metaDiskPathRef,
        long initFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            resRef,
            volDfn,
            storPool,
            blockDevicePathRef,
            metaDiskPathRef,
            initFlags,
            transMgr
        );
    }

    /*
     * used by database drivers and tests
     */
    VolumeData(
        UUID uuid,
        AccessContext accCtx,
        Resource resRef,
        VolumeDefinition volDfnRef,
        StorPool storPoolRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        long initFlags,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        storPool = storPoolRef;
        blockDevicePath = blockDevicePathRef;
        metaDiskPath = metaDiskPathRef;

        dbDriver = LinStor.getVolumeDataDatabaseDriver();

        flags = new VlmFlagsImpl(
            resRef.getObjProt(),
            this,
            dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeConnections = new TransactionMap<>(new HashMap<Volume, VolumeConnection>(), null);
        volumeProps = PropsContainer.getInstance(
            PropsContainer.buildPath(
                resRef.getAssignedNode().getName(),
                resRef.getDefinition().getName(),
                volDfnRef.getVolumeNumber()
            ),
            transMgr
        );

        transObjs = Arrays.asList(
            resource,
            volumeDfn,
            storPool,
            volumeConnections,
            volumeProps,
            flags
        );

        ((ResourceData) resRef).putVolume(accCtx, this);
        ((StorPoolData) storPoolRef).putVolume(accCtx, this);
        ((VolumeDefinitionData) volDfnRef).putVolume(accCtx, this);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public static VolumeData getInstance(
        AccessContext accCtx,
        Resource resRef,
        VolumeDefinition volDfn,
        StorPool storPool,
        String blockDevicePathRef,
        String metaDiskPathRef,
        VlmFlags[] flags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        resRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeData volData = null;

        VolumeDataDatabaseDriver driver = LinStor.getVolumeDataDatabaseDriver();
        volData = driver.load(resRef, volDfn, false, transMgr);

        if (failIfExists && volData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Volume already exists");
        }

        if (volData == null && createIfNotExists)
        {
            volData = new VolumeData(
                accCtx,
                resRef,
                volDfn,
                storPool,
                blockDevicePathRef,
                metaDiskPathRef,
                StateFlagsBits.getMask(flags),
                transMgr
            );
            driver.create(volData, transMgr);
        }
        if (volData != null)
        {
            volData.initialized();
            volData.setConnection(transMgr);
        }
        return volData;
    }

    public static VolumeData getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmUuid,
        Resource rscRef,
        VolumeDefinition vlmDfn,
        StorPool storPoolRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        VlmFlags[] flags,
        SatelliteTransactionMgr transMgr
    )
    {
        VolumeDataDatabaseDriver driver = LinStor.getVolumeDataDatabaseDriver();
        VolumeData vlmData;
        try
        {
            vlmData = driver.load(rscRef, vlmDfn, false, transMgr);
            if (vlmData == null)
            {
                vlmData = new VolumeData(
                    vlmUuid,
                    accCtx,
                    rscRef,
                    vlmDfn,
                    storPoolRef,
                    blockDevicePathRef,
                    metaDiskPathRef,
                    StateFlagsBits.getMask(flags),
                    transMgr
                );
            }
            vlmData.initialized();
            vlmData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }

        return vlmData;
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
        return PropsAccess.secureGetProps(accCtx, resource.getObjProt(), volumeProps);
    }

    @Override
    public Resource getResource()
    {
        checkDeleted();
        return resource;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return volumeDfn;
    }

    @Override
    public VolumeConnection getVolumeConnection(AccessContext accCtx, Volume othervolume)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.get(othervolume);
    }

    @Override
    public void setVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.put(targetVolume, volumeConnection);
        }
        else
        {
            volumeConnections.put(sourceVolume, volumeConnection);
        }
    }

    @Override
    public void removeVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.remove(targetVolume);
        }
        else
        {
            volumeConnections.remove(sourceVolume);
        }
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPool;
    }

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return blockDevicePath;
    }

    @Override
    public String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return metaDiskPath;
    }

    public void setBlockDevicePath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        blockDevicePath = path;
    }

    public void setMetaDiskPath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        metaDiskPath = path;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Volume.VlmFlags.DELETE);
    }


    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);

        ((ResourceData) resource).removeVolume(accCtx, this);
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

    @Override
    public String toString()
    {
        return "Node: '" + resource.getAssignedNode().getName() + "', " +
               "Rsc: '" + resource.getDefinition().getName() + "', " +
               "VlmNr: '" + volumeDfn.getVolumeNumber() + "'";
    }

    @Override
    public Volume.VlmApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new VlmPojo(
                getStorPool(accCtx).getName().getDisplayName(),
                getStorPool(accCtx).getUuid(),
                getVolumeDefinition().getUuid(),
                getUuid(),
                getBlockDevicePath(accCtx),
                getMetaDiskPath(accCtx),
                getVolumeDefinition().getVolumeNumber().value,
                getFlags().getFlagsBits(accCtx),
                getProps(accCtx).map());
    }

    private final class VlmFlagsImpl extends StateFlagsBits<VolumeData, VlmFlags>
    {
        VlmFlagsImpl(
            ObjectProtection objProtRef,
            VolumeData parent,
            StateFlagsPersistence<VolumeData> persistenceRef,
            long initFlags
        )
        {
            super(
                objProtRef,
                parent,
                StateFlagsBits.getMask(
                    VlmFlags.values()
                ),
                persistenceRef,
                initFlags
            );
        }
    }
}
