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
import com.linbit.drbdmanage.storage.StorageException;

public class StorPoolData extends BaseTransactionObject implements StorPool
{
    private final UUID uuid;
    private final StorPoolDefinition storPoolDef;
    private final ObjectProtection objProt;
    private final StorageDriver storDriver;
    private final String storDriverSimpleClassName;
    private final Props props;

//    private final StorPoolDataDatabaseDriver dbDriver;

    StorPoolData(
        AccessContext accCtx,
        StorPoolDefinition storPoolDef,
        TransactionMgr transMgr,
        StorageDriver storDriver,
        String storDriverSimpleClassName,
        SerialGenerator serGen,
        Node nodeRef
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
            storPoolDef,
            transMgr,
            storDriver,
            storDriverSimpleClassName,
            serGen,
            nodeRef
        );
    }

    StorPoolData(
        UUID id,
        ObjectProtection objProtRef,
        StorPoolDefinition storPoolDefRef,
        TransactionMgr transMgr,
        StorageDriver storDriverRef,
        String storDriverSimpleClassNameRef,
        SerialGenerator serGen,
        Node nodeRef
    )
        throws SQLException
    {
        uuid = id;
        storPoolDef = storPoolDefRef;
        storDriver = storDriverRef;
        storDriverSimpleClassName = storDriverSimpleClassNameRef;
        objProt = objProtRef;

//        dbDriver = DrbdManage.getStorPoolDataDatabaseDriver(nodeRef, storPoolDefRef);
        props = SerialPropsContainer.loadContainer(
            DrbdManage.getPropConDatabaseDriver(
                PropsContainer.buildPath(
                    storPoolDefRef.getName(),
                    nodeRef.getName()
                )
            ),
            transMgr,
            serGen
        );

        transObjs = Arrays.<TransactionObject>asList(props);
    }

    public static StorPoolData getInstance(AccessContext accCtx,
        StorPoolDefinition storPoolDefRef,
        TransactionMgr transMgr,
        StorageDriver storDriverRef,
        String storDriverSimpleClassNameRef,
        SerialGenerator serGen,
        Node nodeRef,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        StorPoolData storPoolData = null;
        StorPoolDataDatabaseDriver driver = DrbdManage.getStorPoolDataDatabaseDriver(nodeRef, storPoolDefRef);
        if (transMgr != null)
        {
            if (storDriverRef != null)
            {
                // as transMgr is not null, we are called by the controller.
                // however, only the satellites should have StorageDrivers
                throw new ImplementationError("Controller should not have an instance of StorageDriver", null);
            }
            nodeRef.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            storPoolData = driver.load(transMgr.dbCon, transMgr, serGen);
            if (storPoolData == null && createIfNotExists)
            {
                storPoolData = new StorPoolData(
                    accCtx,
                    storPoolDefRef,
                    transMgr,
                    storDriverRef,
                    storDriverSimpleClassNameRef,
                    serGen,
                    nodeRef
                );
                driver.create(transMgr.dbCon, storPoolData);
            }
        }
        else
        if (createIfNotExists)
        {
            if (storDriverRef == null)
            {
                // no transMgr means we are satellite, thus a storDriver is needed.
                throw new ImplementationError("Satellite should have an instance of StorageDriver", null);
            }
            nodeRef.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            storPoolData = new StorPoolData(
                accCtx,
                storPoolDefRef,
                transMgr,
                storDriverRef,
                storDriverSimpleClassNameRef,
                serGen,
                nodeRef
            );
        }

        nodeRef.addStorPool(accCtx, storPoolData);
        return storPoolData;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public StorPoolName getName()
    {
        return storPoolDef.getName();
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return storPoolDef;
    }

    @Override
    public StorageDriver getDriver(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return storDriver;
    }

    @Override
    public Props getConfiguration(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return props;
    }

    @Override
    public void reconfigureStorageDriver() throws StorageException
    {
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
        return storDriverSimpleClassName;
    }
}
