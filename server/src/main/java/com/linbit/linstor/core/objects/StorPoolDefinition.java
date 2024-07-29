package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.StorPoolDfnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class StorPoolDefinition extends AbsCoreObj<StorPoolDefinition> implements ProtectedObject
{
    public interface InitMaps
    {
        Map<NodeName, StorPool> getStorPoolMap();
    }

    private final StorPoolName name;
    private final ObjectProtection objProt;
    private final StorPoolDefinitionDatabaseDriver dbDriver;
    private final TransactionMap<StorPoolDefinition, NodeName, StorPool> storPools;
    private final Props props;

    StorPoolDefinition(
        UUID id,
        ObjectProtection objProtRef,
        StorPoolName nameRef,
        StorPoolDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<NodeName, StorPool> storPoolsMapRef
    )
        throws DatabaseException
    {
        super(id, transObjFactory, transMgrProviderRef);

        objProt = objProtRef;
        name = nameRef;
        dbDriver = dbDriverRef;
        storPools = transObjFactory.createTransactionMap(this, storPoolsMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(nameRef),
            toStringImpl(),
            LinStorObject.STOR_POOL_DFN
        );

        transObjs = Arrays.<TransactionObject>asList(
            objProt,
            storPools,
            props,
            deleted
        );
        activateTransMgr();
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    public StorPoolName getName()
    {
        checkDeleted();
        return name;
    }

    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.values().iterator();
    }

    public Stream<StorPool> streamStorPools(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.values().stream();
    }

    public void addStorPool(AccessContext accCtx, StorPool storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.put(storPoolData.getNode().getName(), storPoolData);
    }

    public void removeStorPool(AccessContext accCtx, StorPool storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.remove(storPoolData.getNode().getName());
    }

    public @Nullable StorPool getStorPool(AccessContext accCtx, NodeName nodeName) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.get(nodeName);
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, props);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
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

            deleted.set(Boolean.TRUE);
        }
    }

    public StorPoolDefinitionApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return new StorPoolDfnPojo(getUuid(), getName().getDisplayName(), getProps(accCtx).map());
    }

    @Override
    public String toStringImpl()
    {
        return "StorPool: '" + name + "'";
    }

    @Override
    public int compareTo(StorPoolDefinition otherStorPool)
    {
        return getName().compareTo(otherStorPool.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof StorPoolDefinition)
        {
            StorPoolDefinition other = (StorPoolDefinition) obj;
            other.checkDeleted();
            ret = Objects.equals(name, other.name);
        }
        return ret;
    }

}
