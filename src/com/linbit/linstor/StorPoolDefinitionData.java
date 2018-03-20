package com.linbit.linstor;

import com.linbit.linstor.api.pojo.StorPoolDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import javax.inject.Provider;

public class StorPoolDefinitionData extends BaseTransactionObject implements StorPoolDefinition
{
    private final UUID uuid;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final StorPoolName name;
    private final ObjectProtection objProt;
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final TransactionMap<NodeName, StorPool> storPools;
    private final Props props;
    private final TransactionSimpleObject<StorPoolDefinitionData, Boolean> deleted;

    StorPoolDefinitionData(
        UUID id,
        ObjectProtection objProtRef,
        StorPoolName nameRef,
        StorPoolDefinitionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<NodeName, StorPool> storPoolsMapRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);

        uuid = id;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        name = nameRef;
        dbDriver = dbDriverRef;
        storPools = transObjFactory.createTransactionMap(storPoolsMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(nameRef)
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.<TransactionObject>asList(
            objProt,
            storPools,
            props,
            deleted
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
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public StorPoolName getName()
    {
        checkDeleted();
        return name;
    }

    @Override
    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.values().iterator();
    }

    @Override
    public Stream<StorPool> streamStorPools(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.values().stream();
    }

    void addStorPool(AccessContext accCtx, StorPoolData storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.put(storPoolData.getNode().getName(), storPoolData);
    }

    void removeStorPool(AccessContext accCtx, StorPoolData storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.remove(storPoolData.getNode().getName());
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx, NodeName nodeName) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.get(nodeName);
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, props);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            // preventing ConcurrentModificationException
            Collection<StorPool> values = new ArrayList<>(storPools.values());
            for (StorPool storPool : values)
            {
                storPool.delete(accCtx);
            }

            props.delete();

            activateTransMgr();
            objProt.delete(accCtx);
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public StorPoolDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new StorPoolDfnPojo(getUuid(), getName().getDisplayName(), getProps(accCtx).map());
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted storage pool definition");
        }
    }

    @Override
    public String toString()
    {
        return "StorPool: '" + name + "'";
    }
}
