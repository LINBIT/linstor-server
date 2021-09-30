package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;

import java.util.Collection;

public class K8sCrdListToJsonArrayDriver<DATA, LIST_TYPE> implements CollectionDatabaseDriver<DATA, LIST_TYPE>
{

    @Override
    public void insert(DATA parentRef, LIST_TYPE valueRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(parentRef, valueRef, backingCollectionRef);
    }

    @Override
    public void remove(DATA parentRef, LIST_TYPE valueRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(parentRef, valueRef, backingCollectionRef);
    }

    private void update(DATA data, LIST_TYPE ignored, Collection<LIST_TYPE> backingCollection)
        throws DatabaseException
    {

    }
}
