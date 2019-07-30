package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.utils.Pair;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

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

    // Properties container for this volume
    private final Props volumeProps;

    // State flags
    private final StateFlags<VlmFlags> flags;

    private final TransactionMap<Volume.Key, VolumeConnection> volumeConnections;

    private final TransactionSimpleObject<VolumeData, String> devicePath;

    private final TransactionSimpleObject<VolumeData, Long> usableSize;

    private final TransactionSimpleObject<VolumeData, Long> allocatedSize;

    private final VolumeDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeData, Boolean> deleted;

    private final Key vlmKey;

    VolumeData(
        UUID uuid,
        Resource resRef,
        VolumeDefinition volDfnRef,
        long initFlags,
        VolumeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<Volume.Key, VolumeConnection> vlmConnsMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        dbDriver = dbDriverRef;

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
        usableSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        vlmKey = new Key(this);

        transObjs = Arrays.asList(
            resource,
            volumeDfn,
            volumeConnections,
            volumeProps,
            usableSize,
            flags,
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
    public Stream<VolumeConnection> streamVolumeConnections(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.values().stream();
    }

    @Override
    public VolumeConnection getVolumeConnection(AccessContext accCtx, Volume othervolume)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.get(othervolume.getKey());
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
            volumeConnections.put(targetVolume.getKey(), volumeConnection);
        }
        else
        {
            volumeConnections.put(sourceVolume.getKey(), volumeConnection);
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
            volumeConnections.remove(targetVolume.getKey());
        }
        else
        {
            volumeConnections.remove(sourceVolume.getKey());
        }
    }

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public String getDevicePath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return devicePath.get();
    }

    @Override
    public void setDevicePath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        try
        {
            devicePath.set(path);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Volume.VlmFlags.DELETE);
    }

    @Override
    public boolean isUsableSizeSet(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return usableSize.get() != null;
    }

    @Override
    public void setUsableSize(AccessContext accCtx, long size) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);

        try
        {
            usableSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    @Override
    public long getUsableSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return usableSize.get();
    }

    @Override
    public boolean isAllocatedSizeSet(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return allocatedSize.get() != null;
    }

    @Override
    public void setAllocatedSize(AccessContext accCtx, long size) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        try
        {
            allocatedSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    @Override
    public long getAllocatedSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return allocatedSize.get();
    }

    @Override
    public long getEstimatedSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return volumeDfn.getVolumeSize(accCtx);
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
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
    public Volume.VlmApi getApiData(Long allocated, AccessContext accCtx) throws AccessDeniedException
    {
        VolumeNumber vlmNr = getVolumeDefinition().getVolumeNumber();
        List<Pair<String, VlmLayerDataApi>> layerDataList = new ArrayList<>();

        StorPool compatStorPool = null;

        LinkedList<RscLayerObject> rscLayersToExpand = new LinkedList<>();
        rscLayersToExpand.add(resource.getLayerData(accCtx));
        while (!rscLayersToExpand.isEmpty())
        {
            RscLayerObject rscLayerObject = rscLayersToExpand.removeFirst();
            VlmProviderObject vlmProvider = rscLayerObject.getVlmLayerObjects().get(vlmNr);
            if (vlmProvider != null)
            {
                // vlmProvider is null is a layer (like DRBD) does not need for all volumes backing vlmProvider
                // (like in the case of mixed internal and external meta-data)
                layerDataList.add(
                    new Pair<>(
                        vlmProvider.getLayerKind().name(),
                        vlmProvider.asPojo(accCtx)
                    )
                );
            }

            // deprecated - only for compatibility with old versions
            if (rscLayerObject.getResourceNameSuffix().equals("")) // for "" resources vlmProvider always have to exist
            {
                compatStorPool = vlmProvider.getStorPool();
            }

            rscLayersToExpand.addAll(rscLayerObject.getChildren());
        }


        String compatStorPoolName = null;
        DeviceProviderKind compatStorPoolKind = null;
        if (compatStorPool != null)
        {
            compatStorPoolName = compatStorPool.getName().displayValue;
            compatStorPoolKind = compatStorPool.getDeviceProviderKind();
        }

        return new VlmPojo(
            getVolumeDefinition().getUuid(),
            getUuid(),
            getDevicePath(accCtx),
            vlmNr.value,
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            Optional.ofNullable(allocated),
            Optional.ofNullable(usableSize.get()),
            layerDataList,
            compatStorPoolName,
            compatStorPoolKind
        );
    }

    @Override
    public Key getKey()
    {
        return vlmKey;
    }
}
