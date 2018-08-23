package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import javax.inject.Provider;

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

    private final TransactionSimpleObject<VolumeData, StorPool> storPool;

    // Properties container for this volume
    private final Props volumeProps;

    // State flags
    private final StateFlags<VlmFlags> flags;

    private final TransactionMap<Volume, VolumeConnection> volumeConnections;

    private final TransactionSimpleObject<VolumeData, String> backingDiskPath;

    private final TransactionSimpleObject<VolumeData, String> metaDiskPath;

    private final TransactionSimpleObject<VolumeData, Long> realSize;

    private final VolumeDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeData, Boolean> deleted;

    VolumeData(
        UUID uuid,
        Resource resRef,
        VolumeDefinition volDfnRef,
        StorPool storPoolRef,
        String backingDiskPathRef,
        String metaDiskPathRef,
        long initFlags,
        VolumeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<Volume, VolumeConnection> vlmConnsMapRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        backingDiskPath = transObjFactory.createTransactionSimpleObject(this, backingDiskPathRef, null);
        metaDiskPath = transObjFactory.createTransactionSimpleObject(this, metaDiskPathRef, null);
        dbDriver = dbDriverRef;

        storPool = transObjFactory.createTransactionSimpleObject(
            this,
            storPoolRef,
            dbDriver.getStorPoolDriver()
        );

        flags = transObjFactory.createStateFlagsImpl(
            resRef.getObjProt(),
            this,
            VlmFlags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeConnections = transObjFactory.createTransactionMap(vlmConnsMapRef, null);
        volumeProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                resRef.getAssignedNode().getName(),
                resRef.getDefinition().getName(),
                volDfnRef.getVolumeNumber()
            )
        );
        realSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            resource,
            volumeDfn,
            storPool,
            volumeConnections,
            volumeProps,
            realSize,
            flags,
            backingDiskPath,
            metaDiskPath,
            deleted
        );
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
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
    public void setVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
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
    public void removeVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
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
        return storPool.get();
    }

    @Override
    public void setStorPool(AccessContext accCtx, StorPool storPoolRef)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        StorPool oldStorPool = storPool.get();
        if (oldStorPool != null)
        {
            if (oldStorPool.containsVolume(accCtx, this))
            {
                oldStorPool.getFreeSpaceTracker().removingVolume(accCtx, this);
            }
        }
        storPool.set(storPoolRef);
        storPoolRef.putVolume(accCtx, this);
    }

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public String getBackingDiskPath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return backingDiskPath.get();
    }

    @Override
    public String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return metaDiskPath.get();
    }

    @Override
    public void setBackingDiskPath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        try
        {
            backingDiskPath.set(path);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void setMetaDiskPath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        try
        {
            metaDiskPath.set(path);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Volume.VlmFlags.DELETE);
        storPool.get().getFreeSpaceTracker().removingVolume(accCtx, this);
    }

    @Override
    public boolean isRealSizeSet(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return realSize.get() != null;
    }

    @Override
    public void setRealSize(AccessContext accCtx, long size) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);

        try
        {
            realSize.set(size);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    @Override
    public long getRealSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return realSize.get();
    }

    @Override
    public long getEstimatedSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return volumeDfn.getVolumeSize(accCtx);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            resource.getObjProt().requireAccess(accCtx, AccessType.USE);

            // preventing ConcurrentModificationException
            Collection<VolumeConnection> values = new ArrayList<>(volumeConnections.values());
            for (VolumeConnection vlmConn : values)
            {
                vlmConn.delete(accCtx);
            }

            ((ResourceData) resource).removeVolume(accCtx, this);
            if (storPool.get().containsVolume(accCtx, this))
            {
                throw new ImplementationError(
                    "A volume was not removed from its storPool (which includes an update to the freeSpaceMgr)"
                );
            }
            ((VolumeDefinitionData) volumeDfn).removeVolume(accCtx, this);

            volumeProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume");
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
                getBackingDiskPath(accCtx),
                getMetaDiskPath(accCtx),
                getVolumeDefinition().getVolumeNumber().value,
                getVolumeDefinition().getMinorNr(accCtx).value,
                getFlags().getFlagsBits(accCtx),
                getProps(accCtx).map(),
                getStorPool(accCtx).getDriverName(),
                getStorPool(accCtx).getDefinition(accCtx).getUuid(),
                getStorPool(accCtx).getDefinition(accCtx).getProps(accCtx).map(),
                getStorPool(accCtx).getProps(accCtx).map()
        );
    }
}
