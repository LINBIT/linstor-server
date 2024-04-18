package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.GenericDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpMapDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpObjectDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public abstract class AbsSatelliteDbDriver<DATA> implements GenericDatabaseDriver<DATA>
{
    private static final SingleColumnDatabaseDriver<?, ?> NOOP_COLUMN_DRIVER = new NoOpObjectDatabaseDriver<>();
    private static final CollectionDatabaseDriver<?, ?> NOOP_COLLECTION_DRIVER = new NoOpCollectionDatabaseDriver<>();
    private static final MapDatabaseDriver<?, ?, ?> NOOP_MAP_DRIVER = new NoOpMapDatabaseDriver<>();
    private static final StateFlagsPersistence<?> NOOP_FLAG_DRIVER = new NoOpFlagDriver();

    protected AbsSatelliteDbDriver()
    {

    }

    @Override
    public void create(DATA dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void upsert(DATA dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(DATA dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void truncate() throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    protected <PARENT, COL_VALUE> SingleColumnDatabaseDriver<PARENT, COL_VALUE> getNoopColumnDriver()
    {
        return (SingleColumnDatabaseDriver<PARENT, COL_VALUE>) NOOP_COLUMN_DRIVER;
    }

    @SuppressWarnings("unchecked")
    protected <PARENT, ELEMENT> CollectionDatabaseDriver<PARENT, ELEMENT> getNoopCollectionDriver()
    {
        return (CollectionDatabaseDriver<PARENT, ELEMENT>) NOOP_COLLECTION_DRIVER;
    }

    @SuppressWarnings("unchecked")
    protected <PARENT, K, V> MapDatabaseDriver<PARENT, K, V> getNoopMapDriver()
    {
        return (MapDatabaseDriver<PARENT, K, V>) NOOP_MAP_DRIVER;
    }

    @SuppressWarnings("unchecked")
    protected <T> StateFlagsPersistence<T> getNoopFlagDriver()
    {
        return (StateFlagsPersistence<T>) NOOP_FLAG_DRIVER;
    }
}
