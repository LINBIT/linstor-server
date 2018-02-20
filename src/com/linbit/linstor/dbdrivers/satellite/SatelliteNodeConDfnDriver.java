package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.TransactionMgr;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

public class SatelliteNodeConDfnDriver implements NodeConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteNodeConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public NodeConnectionData load(
        Node sourceNode,
        Node targetNode,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )

    {
        NodeConnectionData nodeConnection = null;
        try
        {
            nodeConnection = (NodeConnectionData) sourceNode.getNodeConnection(dbCtx, targetNode);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return nodeConnection;
    }

    @Override
    public List<NodeConnectionData> loadAllByNode(
        Node node,
        TransactionMgr transMgr
    )

    {
        return Collections.emptyList();
    }

    @Override
    public void create(NodeConnectionData nodeConDfnData, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void delete(NodeConnectionData nodeConDfnData, TransactionMgr transMgr)
    {
        // no-op
    }
}
