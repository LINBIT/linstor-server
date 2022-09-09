package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_SUPPORTS_SNAPSHOTS;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class StorPool extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<StorPool>, LinstorDataObject
{
    public interface InitMaps
    {
        Map<String, VlmProviderObject<Resource>> getVolumeMap();

        Map<String, VlmProviderObject<Snapshot>> getSnapshotVolumeMap();
    }

    private final UUID uuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final StorPoolDefinition storPoolDef;
    private final DeviceProviderKind deviceProviderKind;

    private final Props props;
    private final Node node;
    private final StorPoolDatabaseDriver dbDriver;
    private final FreeSpaceTracker freeSpaceTracker;
    private final boolean externalLocking;

    private final TransactionMap<String, VlmProviderObject<Resource>> vlmProviderMap;
    private final TransactionMap<String, VlmProviderObject<Snapshot>> snapVlmProviderMap;

    private final TransactionSimpleObject<StorPool, Boolean> deleted;

    /**
     * This boolean is only asked if the deviceProviderKind is FILE or FILE_THIN. Otherwise
     * the query is forwarded to the deviceProviderKind
     */
    private final TransactionSimpleObject<StorPool, Boolean> supportsSnapshots;

    private final TransactionSimpleObject<StorPool, Boolean> isPmem;
    private final TransactionSimpleObject<StorPool, Boolean> isVDO;

    private ApiCallRcImpl reports;


    StorPool(
        UUID id,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        DeviceProviderKind providerKindRef,
        FreeSpaceTracker freeSpaceTrackerRef,
        boolean externalLockingRef,
        StorPoolDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<String, VlmProviderObject<Resource>> volumeMapRef,
        Map<String, VlmProviderObject<Snapshot>> snapshotVolumeMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(StorPool.class, FreeSpaceTracker.class, freeSpaceTrackerRef);

        uuid = id;
        dbgInstanceId = UUID.randomUUID();
        storPoolDef = storPoolDefRef;
        deviceProviderKind = providerKindRef;
        freeSpaceTracker = freeSpaceTrackerRef;
        node = nodeRef;
        dbDriver = dbDriverRef;
        externalLocking = externalLockingRef;
        vlmProviderMap = transObjFactory.createTransactionMap(volumeMapRef, null);
        snapVlmProviderMap = transObjFactory.createTransactionMap(snapshotVolumeMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName())
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        final boolean isFileProviderKind = providerKindRef == DeviceProviderKind.FILE ||
            providerKindRef == DeviceProviderKind.FILE_THIN;

        supportsSnapshots = transObjFactory.createTransactionSimpleObject(
            this,
            isFileProviderKind ? null : providerKindRef.isSnapshotSupported(),
            null
        );
        isPmem = transObjFactory.createTransactionSimpleObject(this, false, null);
        isVDO = transObjFactory.createTransactionSimpleObject(this, false, null);

        reports = new ApiCallRcImpl();

        transObjs = Arrays.<TransactionObject>asList(
            vlmProviderMap,
            snapVlmProviderMap,
            props,
            deleted,
            freeSpaceTracker
        );
        activateTransMgr();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return uuid;
    }

    public StorPoolName getName()
    {
        checkDeleted();
        return storPoolDef.getName();
    }

    public Node getNode()
    {
        return node;
    }

    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPoolDef;
    }

    public DeviceProviderKind getDeviceProviderKind()
    {
        return deviceProviderKind;
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, node.getObjProt(), storPoolDef.getObjProt(), props);
    }

    public void putVolume(AccessContext accCtx, VlmProviderObject<Resource> vlmProviderObj)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        vlmProviderMap.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);
        freeSpaceTracker.vlmCreating(accCtx, vlmProviderObj);
    }

    public void removeVolume(AccessContext accCtx, VlmProviderObject<Resource> vlmProviderObj)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        freeSpaceTracker.ensureVlmNoLongerCreating(accCtx, vlmProviderObj);

        vlmProviderMap.remove(vlmProviderObj.getVolumeKey());
    }

    public Collection<VlmProviderObject<Resource>> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return vlmProviderMap.values();
    }

    public boolean isPmem()
    {
        return isPmem.get();
    }

    public void setPmem(boolean pmemRef) throws DatabaseException
    {
        isPmem.set(pmemRef);
    }

    public boolean isVDO()
    {
        return isVDO.get();
    }

    public void setVDO(boolean vdo) throws DatabaseException
    {
        isVDO.set(vdo);
    }

    public void putSnapshotVolume(AccessContext accCtx, VlmProviderObject<Snapshot> vlmProviderObj)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        snapVlmProviderMap.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);
        freeSpaceTracker.vlmCreating(accCtx, vlmProviderObj);
    }

    public void removeSnapshotVolume(AccessContext accCtx, VlmProviderObject<Snapshot> vlmProviderObj)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        freeSpaceTracker.ensureVlmNoLongerCreating(accCtx, vlmProviderObj);

        snapVlmProviderMap.remove(vlmProviderObj.getVolumeKey());
    }

    public Collection<VlmProviderObject<Snapshot>> getSnapVolumes(AccessContext accCtx)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return snapVlmProviderMap.values();
    }

    public FreeSpaceTracker getFreeSpaceTracker()
    {
        return freeSpaceTracker;
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            node.getObjProt().requireAccess(accCtx, AccessType.USE);
            storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

            node.removeStorPool(accCtx, this);
            storPoolDef.removeStorPool(accCtx, this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public ApiCallRc getReports()
    {
        return reports;
    }

    @Override
    public void addReports(ApiCallRc apiCallRc)
    {
        reports.addEntries(apiCallRc);
    }

    @Override
    public void clearReports()
    {
        reports = new ApiCallRcImpl();
    }

    public void setSupportsSnapshot(AccessContext accCtx, boolean supportsSnapshotsRef)
        throws AccessDeniedException, DatabaseException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        supportsSnapshots.set(supportsSnapshotsRef);
    }

    public boolean isSnapshotSupported(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        boolean ret;
        if (deviceProviderKind.equals(DeviceProviderKind.FILE) ||
            deviceProviderKind.equals(DeviceProviderKind.FILE_THIN))
        {
            ret = supportsSnapshots.get();
        }
        else
        {
            ret = deviceProviderKind.isSnapshotSupported();
        }
        return supportsSnapshots.get();
    }

    public boolean isSnapshotSupportedInitialized(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return supportsSnapshots.get() != null;
    }

    public boolean isExternalLocking()
    {
        return externalLocking;
    }

    public boolean isShared()
    {
        return freeSpaceTracker.getName().isShared() && !externalLocking;
    }

    public SharedStorPoolName getSharedStorPoolName()
    {
        return freeSpaceTracker.getName();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted storage pool");
        }
    }

    private Map<String, String> getTraits(AccessContext accCtx)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        Map<String, String> traits = new HashMap<>(deviceProviderKind.getStorageDriverKind().getStaticTraits());

        traits.put(
            KEY_STOR_POOL_SUPPORTS_SNAPSHOTS,
            String.valueOf(deviceProviderKind.getStorageDriverKind().isSnapshotSupported())
        );

        return traits;
    }

    @Override
    public String toString()
    {
        return "Node: '" + node.getName() + "', " +
               "StorPool: '" + storPoolDef.getName() + "'";
    }

    @Override
    public int compareTo(StorPool otherStorPool)
    {
        int eq = getNode().compareTo(otherStorPool.getNode());
        if (eq == 0)
        {
            eq = getName().compareTo(otherStorPool.getName());
        }
        return eq;
    }

    public StorPoolApi getApiData(
        Long totalSpaceRef,
        Long freeSpaceRef,
        AccessContext accCtx,
        Long fullSyncId,
        Long updateId
    )
        throws AccessDeniedException
    {
        return new StorPoolPojo(
            getUuid(),
            getNode().getUuid(),
            node.getName().getDisplayName(),
            getName().getDisplayName(),
            getDefinition(accCtx).getUuid(),
            getDeviceProviderKind(),
            getProps(accCtx).cloneMap(),
            getDefinition(accCtx).getProps(accCtx).cloneMap(),
            getTraits(accCtx),
            fullSyncId,
            updateId,
            getFreeSpaceTracker().getName().displayValue,
            Optional.ofNullable(freeSpaceRef),
            Optional.ofNullable(totalSpaceRef),
            getReports(),
            supportsSnapshots.get(),
            isPmem.get(),
            isVDO.get(),
            externalLocking
        );
    }

    /**
     * Identifies a stor pool.
     */
    public static class Key implements Comparable<Key>
    {
        private final NodeName nodeName;

        private final StorPoolName storPoolName;

        public Key(NodeName nodeNameRef, StorPoolName storPoolNameRef)
        {
            nodeName = nodeNameRef;
            storPoolName = storPoolNameRef;
        }

        public Key(StorPool storPool)
        {
            this(storPool.getNode().getName(), storPool.getName());
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public StorPoolName getStorPoolName()
        {
            return storPoolName;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null || getClass() != obj.getClass())
            {
                return false;
            }
            Key that = (Key) obj;
            return Objects.equals(nodeName, that.nodeName) &&
                Objects.equals(storPoolName, that.storPoolName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, storPoolName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = storPoolName.compareTo(other.storPoolName);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Node: '" + nodeName + "' StorPool: '" + storPoolName + "'";
        }
    }
}
