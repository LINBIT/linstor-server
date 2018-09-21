package com.linbit.linstor;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_SUPPORTS_SNAPSHOTS;
import static com.linbit.linstor.api.ApiConsts.NAMESPC_STORAGE_DRIVER;

import com.linbit.ErrorCheck;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;

public class StorPoolData extends BaseTransactionObject implements StorPool
{
    private final UUID uuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final StorPoolDefinition storPoolDef;
    private final StorageDriverKind storageDriverKind;
    private final boolean allowStorageDriverCreation;
    private final Props props;
    private final Node node;
    private final StorPoolDataDatabaseDriver dbDriver;
    private final FreeSpaceTracker freeSpaceTracker;

    private final TransactionMap<String, Volume> volumeMap;

    private final TransactionSimpleObject<StorPoolData, Boolean> deleted;

    private transient StorageDriver storageDriver;

    StorPoolData(
        UUID id,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        StorageDriverKind storageDriverKindRef,
        FreeSpaceTracker freeSpaceTrackerRef,
        boolean allowStorageDriverCreationRef,
        StorPoolDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<String, Volume> volumeMapRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(StorPoolData.class, FreeSpaceTracker.class, freeSpaceTrackerRef);

        uuid = id;
        dbgInstanceId = UUID.randomUUID();
        storPoolDef = storPoolDefRef;
        storageDriverKind = storageDriverKindRef;
        freeSpaceTracker = freeSpaceTrackerRef;
        allowStorageDriverCreation = allowStorageDriverCreationRef;
        node = nodeRef;
        dbDriver = dbDriverRef;
        volumeMap = transObjFactory.createTransactionMap(volumeMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName())
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.<TransactionObject>asList(
            volumeMap,
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
    public StorageDriver getDriver(
        AccessContext accCtx,
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StltConfigAccessor stltCfgAccessor
    )
        throws AccessDeniedException
    {
        if (allowStorageDriverCreation)
        {
            checkDeleted();
            node.getObjProt().requireAccess(accCtx, AccessType.USE);
            if (storageDriver == null)
            {
                storageDriver = storageDriverKind.makeStorageDriver(
                    errorReporter,
                    fileSystemWatch,
                    timer,
                    stltCfgAccessor
                );
            }
        }
        return storageDriver;
    }

    @Override
    public StorageDriverKind getDriverKind()
    {
        return storageDriverKind;
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, node.getObjProt(), storPoolDef.getObjProt(), props);
    }

    @Override
    public void reconfigureStorageDriver(
        StorageDriver storageDriverRef,
        ReadOnlyProps nodeStorageDriverNamespace,
        ReadOnlyProps stltStorageDriverNamespace
    )
        throws StorageException
    {
        checkDeleted();
        if (storageDriverRef.getKind().hasBackingStorage())
        {
            Optional<Props> namespace = props.getNamespace(NAMESPC_STORAGE_DRIVER);
            Map<String, String> storPoolNamespace = namespace.map(Props::map).orElse(Collections.emptyMap());
            Map<String, String> nodeNamespace = nodeStorageDriverNamespace.map();
            Map<String, String> stltNamespace = stltStorageDriverNamespace.map();

            // TODO: this is only a workaround. ReadOnlyProps.map() renders all keys to their absolute path
            // this workaround will simply cut the NAMESPC_STORAGE_DRIVER prefix from the keys

            // one way of properly fix this would involve implementing ReadOnlyPropsConMap-accessor classes
            // just like PropsContainer#PropsconMap inner class (but with exceptions on modification-methods)

            // unfortunately this cannot be extended easily as ReadOnlyProps does not extend PropsContainer.
            int prefixLen = (NAMESPC_STORAGE_DRIVER + "/").length();
            nodeNamespace = cutKeyPrefix(nodeNamespace, prefixLen);
            stltNamespace = cutKeyPrefix(stltNamespace, prefixLen);

            storageDriverRef.setConfiguration(
                storPoolDef.getName().value,
                storPoolNamespace,
                nodeNamespace,
                stltNamespace
            );
        }
    }

    private Map<String, String> cutKeyPrefix(Map<String, String> map, int prefixLen)
    {
        Map<String, String> tmp = new HashMap<>();
        for (Entry<String, String> entry : map.entrySet())
        {
            tmp.put(
                entry.getKey().substring(prefixLen),
                entry.getValue()
            );
        }
        return tmp;
    }

    @Override
    public String getDriverName()
    {
        checkDeleted();
        return storageDriverKind.getDriverName();
    }

    @Override
    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        volumeMap.put(Volume.getVolumeKey(volume), volume);
        freeSpaceTracker.vlmCreating(accCtx, volume);
    }

    @Override
    public void removeVolume(AccessContext accCtx, Volume volume)
        throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        volumeMap.remove(Volume.getVolumeKey(volume));
    }

    @Override
    public boolean containsVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return volumeMap.containsKey(Volume.getVolumeKey(volume));
    }

    @Override
    public Collection<Volume> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return volumeMap.values();
    }

    @Override
    public FreeSpaceTracker getFreeSpaceTracker()
    {
        return freeSpaceTracker;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
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
        Map<String, String> traits = new HashMap<>(storageDriverKind.getStaticTraits());

        traits.put(
            KEY_STOR_POOL_SUPPORTS_SNAPSHOTS,
            String.valueOf(storageDriverKind.isSnapshotSupported())
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
        ArrayList<Volume.VlmApi> vlms = new ArrayList<>();
        for (Volume vlm : getVolumes(accCtx))
        {
            vlms.add(vlm.getApiData(accCtx));
        }
        return new StorPoolPojo(
            getUuid(),
            getNode().getUuid(),
            node.getName().getDisplayName(),
            getName().getDisplayName(),
            getDefinition(accCtx).getUuid(),
            getDriverName(),
            getProps(accCtx).map(),
            getDefinition(accCtx).getProps(accCtx).map(),
            vlms,
            getTraits(accCtx),
            fullSyncId,
            updateId,
            getFreeSpaceTracker().getName().displayValue,
            Optional.ofNullable(freeSpaceRef),
            Optional.ofNullable(totalSpaceRef)
        );
    }
}
