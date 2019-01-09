package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;

import javax.inject.Inject;

public class SatelliteDrbdRscDriver implements DrbdRscDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteDrbdRscDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscObject, NodeId> getNodeIdDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscObject, NodeId>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscObject, Boolean> getDisklessForPeersDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscObject, Boolean>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscObject, Boolean> getDisklessDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscObject, Boolean>) singleColDriver;
    }
}
