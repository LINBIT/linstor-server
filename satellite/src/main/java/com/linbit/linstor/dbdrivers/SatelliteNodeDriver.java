package com.linbit.linstor.dbdrivers;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteNodeDriver implements NodeDatabaseDriver
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
    public StateFlagsPersistence<Node> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<Node>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<Node, Node.Type> getNodeTypeDriver()
    {
        return (SingleColumnDatabaseDriver<Node, Node.Type>) singleColDriver;
    }

    @Override
    public void create(Node node)
    {
        // no-op
    }

    @Override
    public void delete(Node node)
    {
        // no-op
    }
}
