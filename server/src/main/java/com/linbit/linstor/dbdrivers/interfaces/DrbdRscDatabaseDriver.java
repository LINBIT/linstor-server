package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.storage.layer.data.DrbdRscData;

public interface DrbdRscDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdRscData, NodeId> getNodeIdDriver();

    SingleColumnDatabaseDriver<DrbdRscData, Boolean> getDisklessForPeersDriver();

    SingleColumnDatabaseDriver<DrbdRscData, Boolean> getDisklessDriver();
}
