package com.linbit.linstor.api.interfaces.serializer;

import java.io.IOException;
import java.util.Set;

import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;

public interface CtrlFullSyncSerializer
{
    byte[] getData(int msgId, Set<Node> nodes, Set<StorPool> storPools, Set<Resource> resources)
        throws IOException;
}
