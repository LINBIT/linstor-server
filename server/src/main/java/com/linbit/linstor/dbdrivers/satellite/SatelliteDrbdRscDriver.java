package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDatabaseDriver;
import com.linbit.linstor.storage2.layer.data.DrbdRscData;

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
    public SingleColumnDatabaseDriver<DrbdRscData, NodeId> getNodeIdDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscData, NodeId>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscData, Boolean> getDisklessForPeersDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscData, Boolean>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscData, Boolean> getDisklessDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscData, Boolean>) singleColDriver;
    }
}
