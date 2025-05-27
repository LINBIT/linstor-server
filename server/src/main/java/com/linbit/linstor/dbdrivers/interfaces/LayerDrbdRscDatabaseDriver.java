package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;

public interface LayerDrbdRscDatabaseDriver extends AbsLayerDataDatabaseDriver<DrbdRscData<?>>
{
    StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence();

    SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver();

    CollectionDatabaseDriver<DrbdRscData<?>, TcpPortNumber> getTcpPortDriver();
}
