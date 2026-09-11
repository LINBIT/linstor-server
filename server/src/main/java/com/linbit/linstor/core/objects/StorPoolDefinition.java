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
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class StorPoolDefinition extends AbsCoreObj<StorPoolDefinition>
{
    public interface InitMaps
    {
        Map<NodeName, StorPool> getStorPoolMap();
    }

    private final StorPoolName name;
    private final StorPoolDefinitionDatabaseDriver dbDriver;
    private final TransactionMap<StorPoolDefinition, NodeName, StorPool> storPools;
    private final Props props;

    StorPoolDefinition(
        UUID id,
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

        name = nameRef;
        dbDriver = dbDriverRef;
        storPools = transObjFactory.createTransactionMap(this, storPoolsMapRef, null);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(nameRef),
            toStringImpl(),
            LinStorObject.STOR_POOL_DFN
        );

        transObjs = Arrays.<TransactionObject>asList(
            storPools,
            props,
            deleted
        );
        activateTransMgr();
    }

    public StorPoolName getName()
    {
        checkDeleted();
        return name;
    }

    public Iterator<StorPool> iterateStorPools()
    {
        checkDeleted();
        return storPools.values().iterator();
    }

    public Stream<StorPool> streamStorPools()
    {
        checkDeleted();
        return storPools.values().stream();
    }

    public void addStorPool(StorPool storPoolData)
    {
        checkDeleted();
        storPools.put(storPoolData.getNode().getName(), storPoolData);
    }

    public void removeStorPool(StorPool storPoolData)
    {
        checkDeleted();
        storPools.remove(storPoolData.getNode().getName());
    }

    public @Nullable StorPool getStorPool(NodeName nodeName)
    {
        checkDeleted();
        return storPools.get(nodeName);
    }

    public Props getProps()
    {
        checkDeleted();
        return props;
    }

    @Override
    public void delete()
        throws DatabaseException
    {
        if (!deleted.get())
        {

            // preventing ConcurrentModificationException
            Collection<StorPool> values = new ArrayList<>(storPools.values());
            for (StorPool storPool : values)
            {
                storPool.delete();
            }

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    public StorPoolDefinitionApi getApiData()
    {
        checkDeleted();
        return new StorPoolDfnPojo(getUuid(), getName().getDisplayName(), getProps().cloneMap());
    }

    @Override
    public String toStringImpl()
    {
        return "StorPoolDfn: '" + name + "'";
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
        else if (obj instanceof StorPoolDefinition other)
        {
            other.checkDeleted();
            ret = Objects.equals(name, other.name);
        }
        return ret;
    }

}
