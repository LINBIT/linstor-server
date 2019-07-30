package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteNodeDriver implements NodeDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final CoreModule.NodesMap nodesMap;

    @Inject
    public SatelliteNodeDriver(CoreModule.NodesMap nodesMapRef)
    {
        nodesMap = nodesMapRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<NodeData> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<NodeData>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<NodeData, Node.NodeType> getNodeTypeDriver()
    {
        return (SingleColumnDatabaseDriver<NodeData, Node.NodeType>) singleColDriver;
    }

    @Override
    public void create(NodeData node)
    {
        // no-op
    }

    @Override
    public void delete(NodeData node)
    {
        // no-op
    }
}
