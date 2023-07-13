package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerDrbdRscDbDriver
    extends AbsSatelliteDbDriver<DrbdRscData<?>>
    implements LayerDrbdRscDatabaseDriver
{
    private final SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> nodeIdDriver;
    private final StateFlagsPersistence<DrbdRscData<?>> flagsDriver;
    private final LayerResourceIdDatabaseDriver noopResourceLayerIdDriver;

    @Inject
    public SatelliteLayerDrbdRscDbDriver(SatelliteLayerResourceIdDriver stltLayerRscIdDriverRef)
    {
        noopResourceLayerIdDriver = stltLayerRscIdDriverRef;
        nodeIdDriver = getNoopColumnDriver();
        flagsDriver = getNoopFlagDriver();
    }

    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver()
    {
        return nodeIdDriver;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }
}
