package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Node.Type;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteNodeDriver
    extends AbsSatelliteDbDriver<Node>
    implements NodeDatabaseDriver
{
    private final StateFlagsPersistence<Node> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<Node, Type> nodeTypeDriver;

    @Inject
    public SatelliteNodeDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        nodeTypeDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<Node> getStateFlagPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Node, Node.Type> getNodeTypeDriver()
    {
        return nodeTypeDriver;
    }
}
