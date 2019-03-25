package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Provider;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinitionData extends BaseTransactionObject implements ResourceDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Resource name
    private final ResourceName resourceName;

    // User suggested name
    private final byte[] externalName;

    // Volumes of the resource
    private final TransactionMap<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private final TransactionMap<NodeName, Resource> resourceMap;

    // Snapshots from this resource definition
    private final TransactionMap<SnapshotName, SnapshotDefinition> snapshotDfnMap;

    // State flags
    private final StateFlags<RscDfnFlags> flags;

    // Object access controls
    private final ObjectProtection objProt;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    private final ResourceDefinitionDataDatabaseDriver dbDriver;

    private final TransactionMap<DeviceLayerKind, RscDfnLayerObject> layerStorage;

    private final TransactionSimpleObject<ResourceDefinitionData, Boolean> deleted;

    private final TransactionList<ResourceDefinitionData, DeviceLayerKind> layerStack;

    ResourceDefinitionData(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceName resName,
        byte[] extName,
        long initialFlags,
        List<DeviceLayerKind> layerStackRef,
        ResourceDefinitionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
        Map<NodeName, Resource> rscMapRef,
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef,
        Map<DeviceLayerKind, RscDfnLayerObject> layerDataMapRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);

        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ObjectProtection.class, objProtRef);
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        resourceName = resName;
        externalName = extName;
        dbDriver = dbDriverRef;
        volumeMap = transObjFactory.createTransactionMap(vlmDfnMapRef, null);
        resourceMap = transObjFactory.createTransactionMap(rscMapRef, null);
        snapshotDfnMap = transObjFactory.createTransactionMap(snapshotDfnMapRef, null);
        layerStack = transObjFactory.createTransactionListPrimitive(
            this,
            layerStackRef,
            dbDriver.getLayerStackDriver()
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        rscDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resName)
        );
        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            RscDfnFlags.class,
            dbDriver.getStateFlagsPersistence(),
            initialFlags
        );

        layerStorage = transObjFactory.createTransactionMap(layerDataMapRef, null);

        transObjs = Arrays.asList(
            flags,
            objProt,
            volumeMap,
            resourceMap,
            rscDfnProps,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        checkDeleted();
        return resourceName;
    }

    @Override
    public byte[] getExternalName()
    {
        checkDeleted();
        return externalName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }

    synchronized void putVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.put(volDfn.getVolumeNumber(), volDfn);
    }

    synchronized void removeVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.remove(volDfn.getVolumeNumber());
    }

    @Override
    public int getVolumeDfnCount(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.size();
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    @Override
    public Stream<VolumeDefinition> streamVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().stream();
    }

    @Override
    public int getResourceCount()
    {
        return resourceMap.size();
    }

    @Override
    public int diskfullCount(AccessContext accCtx) throws AccessDeniedException
    {
        int count = 0;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            if (rsc.getStateFlags().isUnset(accCtx, Resource.RscFlags.DISKLESS))
            {
                count++;
            }
        }
        return count;
    }

    @Override
    public Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().iterator();
    }

    @Override
    public Stream<Resource> streamResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().stream();
    }

    @Override
    public void copyResourceMap(
        AccessContext accCtx, Map<NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(resourceMap);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(clNodeName);
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getAssignedNode().getName(), resRef);
    }

    void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getAssignedNode().getName());
    }

    @Override
    public void addSnapshotDfn(AccessContext accCtx, SnapshotDefinition snapshotDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.put(snapshotDfn.getName(), snapshotDfn);
    }

    @Override
    public SnapshotDefinition getSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.get(snapshotName);
    }

    @Override
    public Collection<SnapshotDefinition> getSnapshotDfns(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.values();
    }

    @Override
    public void removeSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.remove(snapshotName);
    }

    @Override
    public StateFlags<RscDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public boolean hasDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        boolean hasDiskless = false;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            hasDiskless = rsc.getStateFlags().isSet(accCtx, Resource.RscFlags.DISKLESS);
            if (hasDiskless)
            {
                break;
            }
        }
        return hasDiskless;
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getFlags().enableFlags(accCtx, RscDfnFlags.DELETE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RscDfnLayerObject> T setLayerData(AccessContext accCtx, T rscDfnLayerData)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return (T) layerStorage.put(rscDfnLayerData.getLayerKind(), rscDfnLayerData);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RscDfnLayerObject> T getLayerData(AccessContext accCtx, DeviceLayerKind kind)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return (T) layerStorage.get(kind);
    }

    public void removeLayerData(AccessContext accCtx, DeviceLayerKind kind)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        layerStorage.remove(kind).delete();
        for (VolumeDefinition vlmDfn : volumeMap.values())
        {
            ((VolumeDefinitionData) vlmDfn).removeLayerData(accCtx, kind);
        }
    }

    @Override
    public void setLayerStack(AccessContext accCtx, List<DeviceLayerKind> list)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        layerStack.clear();
        layerStack.addAll(list);
    }

    @Override
    public List<DeviceLayerKind> getLayerStack(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return layerStack;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            // Shallow copy the resource collection because calling delete results in elements being removed from it
            Collection<Resource> resources = new ArrayList<>(resourceMap.values());
            for (Resource rsc : resources)
            {
                rsc.delete(accCtx);
            }

            // Shallow copy the volume definition collection because calling delete results in elements being removed
            // from it
            Collection<VolumeDefinition> volumeDefinitions = new ArrayList<>(volumeMap.values());
            for (VolumeDefinition volumeDefinition : volumeDefinitions)
            {
                volumeDefinition.delete(accCtx);
            }

            rscDfnProps.delete();

            for (RscDfnLayerObject rscDfnLayerObject : layerStorage.values())
            {
                rscDfnLayerObject.delete();
            }

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (isDeleted())
        {
            throw new AccessToDeletedDataException("Access to deleted resource definition");
        }
    }

    @Override
    public ResourceDefinition.RscDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        ArrayList<VolumeDefinition.VlmDfnApi> vlmDfnList = new ArrayList<>();
        Iterator<VolumeDefinition> vlmDfnIter = iterateVolumeDfn(accCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vd = vlmDfnIter.next();
            vlmDfnList.add(vd.getApiData(accCtx));
        }

        /*
         * Satellite should not care about this layerData (and especially not about its ordering)
         * as the satellite should only take the resource's tree-structured layerData into account
         *
         * This means, this serialization is basically only for clients.
         *
         * Sorting an enum by default orders by its ordinal number, not alphanumerically.
         */
        TreeSet<DeviceLayerKind> sortedLayerStack = new TreeSet<>(layerStack);
        sortedLayerStack.addAll(layerStorage.keySet());

        List<Pair<String, RscDfnLayerDataApi>> layerData = new ArrayList<>();
        for (DeviceLayerKind kind : sortedLayerStack)
        {
            RscDfnLayerObject rscDfnLayerObject = layerStorage.get(kind);
            layerData.add(
                new Pair<>(
                    kind.name(),
                    rscDfnLayerObject == null ? null : rscDfnLayerObject.getApiData(accCtx)
                )
            );
        }

        return new RscDfnPojo(
            getUuid(),
            getName().getDisplayName(),
            getExternalName(),
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            vlmDfnList,
            layerData
        );
    }

    @Override
    public Optional<Resource> anyResourceInUse(AccessContext accCtx)
        throws AccessDeniedException
    {
        Resource rscInUse = null;
        Iterator<Resource> rscInUseIterator = iterateResource(accCtx);
        while (rscInUseIterator.hasNext() && rscInUse == null)
        {
            Resource rsc = rscInUseIterator.next();

            Peer nodePeer = rsc.getAssignedNode().getPeer(accCtx);
            if (nodePeer != null)
            {
                Boolean inUse;
                try (LockGuard ignored = LockGuard.createLocked(nodePeer.getSatelliteStateLock().readLock()))
                {
                    inUse = nodePeer.getSatelliteState().getFromResource(resourceName, SatelliteResourceState::isInUse);
                }
                if (inUse != null && inUse)
                {
                    rscInUse = rsc;
                }
            }
        }
        return Optional.ofNullable(rscInUse);
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + resourceName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
