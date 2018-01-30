package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageDriverLoader;
import com.linbit.linstor.storage.StorageException;
import java.util.ArrayList;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_SUPPORTS_SNAPSHOTS;

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

    private final TransactionMap<String, Volume> volumeMap;

    private final TransactionSimpleObject<StorPoolData, Boolean> deleted;

    /*
     * used only by getInstance
     */
    private StorPoolData(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDef,
        String storageDriverName,
        boolean allowStorageDriverCreation,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            nodeRef,
            storPoolDef,
            storageDriverName,
            allowStorageDriverCreation,
            transMgr
        );
    }

    /*
     * used by dbDrivers and tests
     */
    StorPoolData(
        UUID id,
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storageDriverName,
        boolean allowStorageDriverCreationRef,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        uuid = id;
        dbgInstanceId = UUID.randomUUID();
        storPoolDef = storPoolDefRef;
        storageDriverKind = StorageDriverLoader.getKind(storageDriverName);
        allowStorageDriverCreation = allowStorageDriverCreationRef;
        node = nodeRef;
        volumeMap = new TransactionMap<>(new TreeMap<String, Volume>(), null);

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName()),
            transMgr
        );
        deleted = new TransactionSimpleObject<>(this, false, null);

        dbDriver = LinStor.getStorPoolDataDatabaseDriver();

        transObjs = Arrays.<TransactionObject>asList(
            volumeMap,
            props,
            deleted
        );

        ((NodeData) nodeRef).addStorPool(accCtx, this);
        ((StorPoolDefinitionData)storPoolDefRef).addStorPool(accCtx, this);
    }

    /*
     * used by SatellitedummyStorPoolData ONLY
     */
    protected StorPoolData()
    {
        uuid = null;
        dbgInstanceId = UUID.randomUUID();
        storPoolDef = null;
        storageDriverKind = null;
        allowStorageDriverCreation = false;
        props = null;
        node = null;
        dbDriver = null;
        volumeMap = null;
        deleted = null;

        transObjs = Collections.emptyList();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }


    public static StorPoolData getInstance(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        nodeRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDefRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;
        StorPoolDataDatabaseDriver driver = LinStor.getStorPoolDataDatabaseDriver();

        storPoolData = driver.load(nodeRef, storPoolDefRef, false, transMgr);

        if (failIfExists && storPoolData != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        if (storPoolData == null && createIfNotExists)
        {
            storPoolData = new StorPoolData(
                accCtx,
                nodeRef,
                storPoolDefRef,
                storDriverSimpleClassNameRef,
                false,
                transMgr
            );
            driver.create(storPoolData, transMgr);
        }
        if (storPoolData != null)
        {
            storPoolData.initialized();
            storPoolData.setConnection(transMgr);
        }
        return storPoolData;
    }

    public static StorPoolData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        SatelliteTransactionMgr transMgr,
        SatelliteCoreServices stltCoreSvcs
    )
        throws ImplementationError
    {
        StorPoolData storPoolData = null;
        StorPoolDataDatabaseDriver driver = LinStor.getStorPoolDataDatabaseDriver();

        try
        {
            storPoolData = driver.load(nodeRef, storPoolDefRef, false, transMgr);
            if (storPoolData == null)
            {
                StorageDriverKind storageDriverKind =
                    StorageDriverLoader.getKind(storDriverSimpleClassNameRef);

                StorageDriver storageDriver = storageDriverKind.makeStorageDriver();
                storageDriver.initialize(stltCoreSvcs);

                storPoolData = new StorPoolData(
                    uuid,
                    accCtx,
                    nodeRef,
                    storPoolDefRef,
                    storDriverSimpleClassNameRef,
                    true,
                    transMgr
                );
            }
            storPoolData.initialized();
            storPoolData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }


        return storPoolData;
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
    public StorageDriver createDriver(AccessContext accCtx, SatelliteCoreServices coreSvc)
        throws AccessDeniedException
    {
        StorageDriver storageDriver = null;
        if (allowStorageDriverCreation)
        {
            checkDeleted();
            node.getObjProt().requireAccess(accCtx, AccessType.USE);
            storageDriver = storageDriverKind.makeStorageDriver();
            storageDriver.initialize(coreSvc);
        }
        return storageDriver;
    }

    @Override
    public StorageDriverKind getDriverKind(AccessContext accCtx)
        throws AccessDeniedException
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
    public void reconfigureStorageDriver(StorageDriver storageDriver) throws StorageException
    {
        checkDeleted();
        try
        {
            Map<String, String> map = props.getNamespace(STORAGE_DRIVER_PROP_NAMESPACE).map();
            // we could check storDriver for null here, but if it is null, we would throw an implExc anyways
            // so just let java throw the nullpointer exception
            storageDriver.setConfiguration(map);
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Hard coded constant cause an invalid key exception", invalidKeyExc);
        }
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

        volumeMap.put(getVolumeKey(volume), volume);
    }

    @Override
    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        volumeMap.remove(getVolumeKey(volume));
    }

    @Override
    public Collection<Volume> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return volumeMap.values();
    }

    private String getVolumeKey(Volume volume)
    {
        NodeName nodeName = volume.getResource().getAssignedNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        VolumeNumber volNr = volume.getVolumeDefinition().getVolumeNumber();
        return nodeName.value + "/" + rscName.value + "/" + volNr.value;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        ((NodeData) node).removeStorPool(accCtx, this);
        ((StorPoolDefinitionData) storPoolDef).removeStorPool(accCtx, this);
        dbDriver.delete(this, transMgr);
        deleted.set(true);
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new ImplementationError("Access to deleted storage pool", null);
        }
    }

    private Map<String, String> getTraits(AccessContext accCtx)
        throws AccessDeniedException
    {
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
    public StorPoolApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
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
            updateId
        );
    }


}
