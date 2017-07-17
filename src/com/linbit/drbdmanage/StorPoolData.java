package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.storage.StorageDriver;
import com.linbit.drbdmanage.storage.StorageDriverUtils;
import com.linbit.drbdmanage.storage.StorageException;

public class StorPoolData extends BaseTransactionObject implements StorPool
{
    private final UUID uuid;
    private final StorPoolDefinition storPoolDef;
    private final ObjectProtection objProt;
    private final StorageDriver storDriver;
    private final String storDriverSimpleClassName;
    private final Props props;
    private final StorPoolDataDatabaseDriver dbDriver;

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
        SerialGenerator serGen,
        TransactionMgr transMgr
    )
        throws AccessDeniedException, SQLException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPathSP(storPoolDef.getName()),
                true
            ),
            nodeRef,
            storPoolDef,
            storDriver,
            storDriverSimpleClassName,
            serGen,
            transMgr
        );
    }

    /*
     * used by dbDrivers and tests
     */
    StorPoolData(
        UUID id,
        ObjectProtection objProtRef,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        StorageDriver storDriverRef,
        String storDriverSimpleClassNameRef,
        SerialGenerator serGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        uuid = id;
        storPoolDef = storPoolDefRef;
        storDriver = storDriverRef;
        storDriverSimpleClassName = storDriverSimpleClassNameRef;
        objProt = objProtRef;

        props = SerialPropsContainer.getInstance(
            DrbdManage.getPropConDatabaseDriver(
                PropsContainer.buildPath(
                    storPoolDefRef.getName(),
                    nodeRef.getName()
                )
            ),
            transMgr,
            serGen
        );

        dbDriver = DrbdManage.getStorPoolDataDatabaseDriver(nodeRef, storPoolDefRef);

        transObjs = Arrays.<TransactionObject>asList(props);
    }

    public static StorPoolData getInstance(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        SerialGenerator serGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        StorPoolData storPoolData = null;
        StorPoolDataDatabaseDriver driver = DrbdManage.getStorPoolDataDatabaseDriver(nodeRef, storPoolDefRef);
        if (transMgr != null)
        {
            storPoolData = driver.load(transMgr.dbCon, transMgr, serGen);
            if (storPoolData == null && createIfNotExists)
            {
                storPoolData = new StorPoolData(
                    accCtx,
                    nodeRef,
                    storPoolDefRef,
                    null,
                    storDriverSimpleClassNameRef,
                    serGen,
                    transMgr
                );
                driver.create(transMgr.dbCon, storPoolData);
            }
        }
        else
        if (createIfNotExists)
        {
            storPoolData = new StorPoolData(
                accCtx,
                nodeRef,
                storPoolDefRef,
                // TODO: should every StorPool create a new storDriver instance?
                StorageDriverUtils.createInstance(storDriverSimpleClassNameRef),
                storDriverSimpleClassNameRef,
                serGen,
                transMgr
            );
        }

        if (storPoolData != null)
        {
            ((NodeData) nodeRef).addStorPool(accCtx, storPoolData);

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
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return storPoolDef;
    }

    @Override
    public StorageDriver getDriver(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return storDriver;
    }

    @Override
    public Props getConfiguration(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return props;
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
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(dbCon);
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
