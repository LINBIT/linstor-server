package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;

import javax.inject.Inject;

public class SatelliteLayerDrbdRscDbDriver implements LayerDrbdRscDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final StateFlagsPersistence<?> noopStateFlagsDriver = new NoOpFlagDriver();
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteLayerResourceIdDriver();

    @Inject
    public SatelliteLayerDrbdRscDbDriver()
    {
    }

    @Override
    public void create(DrbdRscData<?> drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(DrbdRscData<?> drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return (StateFlagsPersistence<DrbdRscData<?>>) noopStateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId>) noopSingleColDriver;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}

