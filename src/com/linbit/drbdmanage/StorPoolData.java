package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.storage.StorageDriver;
import com.linbit.drbdmanage.storage.StorageDriverUtils;
import com.linbit.drbdmanage.storage.StorageException;

public class StorPoolData extends BaseTransactionObject implements StorPool
{
    private final UUID uuid;
    private final StorPoolDefinition storPoolDef;
    private final StorageDriver storDriver;
    private final String storDriverSimpleClassName;
    private final Props props;
    private final Node node;
    private final StorPoolDataDatabaseDriver dbDriver;

    private final TransactionMap<String, Volume> volumeMap;

    private boolean deleted = false;

    /*
     * used only by getInstance
     */
    private StorPoolData(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDef,
        StorageDriver storDriver,
        String storDriverSimpleClassName,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            nodeRef,
            storPoolDef,
            storDriver,
            storDriverSimpleClassName,
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
        StorageDriver storDriverRef,
        String storDriverSimpleClassNameRef,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        uuid = id;
        storPoolDef = storPoolDefRef;
        storDriver = storDriverRef;
        storDriverSimpleClassName = storDriverSimpleClassNameRef;
        node = nodeRef;
        volumeMap = new TransactionMap<>(new TreeMap<String, Volume>(), null);

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName()),
            transMgr
        );

        dbDriver = DrbdManage.getStorPoolDataDatabaseDriver();

        transObjs = Arrays.<TransactionObject>asList(
            volumeMap,
            props
        );

        ((NodeData) nodeRef).addStorPool(accCtx, this);
        ((StorPoolDefinitionData)storPoolDefRef).addStorPool(accCtx, this);
    }

    public static StorPoolData getInstance(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        TransactionMgr transMgr,
        boolean createStorageDriverInstance,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, ClassNotFoundException, InstantiationException, IllegalAccessException, DrbdDataAlreadyExistsException
    {
        nodeRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDefRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;
        StorPoolDataDatabaseDriver driver = DrbdManage.getStorPoolDataDatabaseDriver();

        storPoolData = driver.load(nodeRef, storPoolDefRef, false, transMgr);

        if (failIfExists && storPoolData != null)
        {
            throw new DrbdDataAlreadyExistsException("The StorPool already exists");
        }

        if (storPoolData == null && createIfNotExists)
        {
            StorageDriver storDriver = null;
            if (createStorageDriverInstance)
            {
                storDriver = StorageDriverUtils.createInstance(storDriverSimpleClassNameRef);
            }
            storPoolData = new StorPoolData(
                accCtx,
                nodeRef,
                storPoolDefRef,
                storDriver,
                storDriverSimpleClassNameRef,
                transMgr
            );
            driver.create(storPoolData, transMgr);
        }
        if (storPoolData != null)
        {
            storPoolData.initialized();
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
    public StorageDriver getDriver(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        return storDriver;
    }

    @Override
    public Props getConfiguration(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, node.getObjProt(), props);
    }

    @Override
    public void reconfigureStorageDriver() throws StorageException
    {
        checkDeleted();
        try
        {
            Map<String, String> map = props.getNamespace(STORAGE_DRIVER_PROP_NAMESPACE).map();
            // we could check storDriver for null here, but if it is null, we would throw an implExc anyways
            // so just let java throw the nullpointer exception
            storDriver.setConfiguration(map);
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
        return storDriverSimpleClassName;
    }

    @Override
    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        volumeMap.put(getVolumeKey(accCtx, volume), volume);
    }

    @Override
    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        volumeMap.remove(getVolumeKey(accCtx, volume));
    }

    @Override
    public Collection<Volume> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return volumeMap.values();
    }

    private String getVolumeKey(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        NodeName nodeName = volume.getResource().getAssignedNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        VolumeNumber volNr = volume.getVolumeDefinition().getVolumeNumber(accCtx);
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
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
    }
}
