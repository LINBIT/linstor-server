package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;

public interface DrbdRscDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdRscObject, NodeId> getNodeIdDriver();

    SingleColumnDatabaseDriver<DrbdRscObject, Boolean> getDisklessForPeersDriver();

    SingleColumnDatabaseDriver<DrbdRscObject, Boolean> getDisklessDriver();
}
