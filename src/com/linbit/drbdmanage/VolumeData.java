package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
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
public class VolumeData extends BaseTransactionObject implements Volume
{
    // Object identifier
    private final UUID objId;

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

    private final String blockDevicePath;

    private final String metaDiskPath;

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
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        storPool = storPoolRef;
        blockDevicePath = blockDevicePathRef;
        metaDiskPath = metaDiskPathRef;

        dbDriver = DrbdManage.getVolumeDataDatabaseDriver();

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
        throws SQLException, AccessDeniedException, DrbdDataAlreadyExistsException
    {
        resRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        VolumeData volData = null;

        VolumeDataDatabaseDriver driver = DrbdManage.getVolumeDataDatabaseDriver();
        volData = driver.load(resRef, volDfn, false, transMgr);

        if (failIfExists && volData != null)
        {
            throw new DrbdDataAlreadyExistsException("The Volume already exists");
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
        VolumeDataDatabaseDriver driver = DrbdManage.getVolumeDataDatabaseDriver();
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
            volumeConnections.remove(targetVolume, volumeConnection);
        }
        else
        {
            volumeConnections.remove(sourceVolume, volumeConnection);
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
