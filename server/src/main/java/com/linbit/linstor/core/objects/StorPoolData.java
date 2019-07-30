package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
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
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_SUPPORTS_SNAPSHOTS;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class StorPoolData extends BaseTransactionObject implements StorPool
{
    private final UUID uuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final StorPoolDefinition storPoolDef;
    private final DeviceProviderKind deviceProviderKind;

    private final Props props;
    private final Node node;
    private final StorPoolDataDatabaseDriver dbDriver;
    private final FreeSpaceTracker freeSpaceTracker;

    private final TransactionMap<String, VlmProviderObject> vlmProviderMap;

    private final TransactionSimpleObject<StorPoolData, Boolean> deleted;

    /**
     * This boolean is only asked if the deviceProviderKind is FILE or FILE_THIN. Otherwise
     * the query is forwarded to the deviceProviderKind
     */
    private final TransactionSimpleObject<StorPoolData, Boolean> supportsSnapshots;

    private ApiCallRcImpl reports;

    StorPoolData(
        UUID id,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        DeviceProviderKind providerKindRef,
        FreeSpaceTracker freeSpaceTrackerRef,
        StorPoolDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<String, VlmProviderObject> volumeMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(StorPoolData.class, FreeSpaceTracker.class, freeSpaceTrackerRef);

        uuid = id;
        dbgInstanceId = UUID.randomUUID();
        storPoolDef = storPoolDefRef;
        deviceProviderKind = providerKindRef;
        freeSpaceTracker = freeSpaceTrackerRef;
        node = nodeRef;
        dbDriver = dbDriverRef;
        vlmProviderMap = transObjFactory.createTransactionMap(volumeMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName())
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
        supportsSnapshots = transObjFactory.createTransactionSimpleObject(this, false, null);

        reports = new ApiCallRcImpl();

        transObjs = Arrays.<TransactionObject>asList(
            vlmProviderMap,
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

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return uuid;
    }

    @Override
    public StorPoolName getName()
    {
        checkDeleted();
        return storPoolDef.getName();
    }

    @Override
    public Node getNode()
    {
        return node;
    }

    @Override
    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPoolDef;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return deviceProviderKind;
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, node.getObjProt(), storPoolDef.getObjProt(), props);
    }

    @Override
    public void putVolume(AccessContext accCtx, VlmProviderObject vlmProviderObj) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        vlmProviderMap.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);
        freeSpaceTracker.vlmCreating(accCtx, vlmProviderObj);
    }

    @Override
    public void removeVolume(AccessContext accCtx, VlmProviderObject vlmProviderObj)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        freeSpaceTracker.ensureVlmNoLongerCreating(accCtx, vlmProviderObj);

        vlmProviderMap.remove(vlmProviderObj.getVolumeKey());
    }

    @Override
    public Collection<VlmProviderObject> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return vlmProviderMap.values();
    }

    @Override
    public FreeSpaceTracker getFreeSpaceTracker()
    {
        return freeSpaceTracker;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            node.getObjProt().requireAccess(accCtx, AccessType.USE);
            storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

            ((NodeData) node).removeStorPool(accCtx, this);
            ((StorPoolDefinitionData) storPoolDef).removeStorPool(accCtx, this);
            freeSpaceTracker.remove(accCtx, this);

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

    @Override
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
            getProps(accCtx).map(),
            getDefinition(accCtx).getProps(accCtx).map(),
            getTraits(accCtx),
            fullSyncId,
            updateId,
            getFreeSpaceTracker().getName().displayValue,
            Optional.ofNullable(freeSpaceRef),
            Optional.ofNullable(totalSpaceRef),
            getReports()
        );
    }
}
