package com.linbit.linstor.systemstarter;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.tasks.ReconnectorTask;

import java.util.Collection;

public class ConnectNodesInitializer implements StartupInitializer
{
    private ErrorReporter errorLog;
    private CoreModule.NodesMap nodesMap;
    private ReconnectorTask reconnectorTask;
    private AccessContext initCtx;

    public ConnectNodesInitializer(
        ErrorReporter errorLogRef,
        CoreModule.NodesMap nodesMapRef,
        ReconnectorTask reconnectorTaskRef,
        AccessContext initCtxRef
    )
    {
        errorLog = errorLogRef;
        nodesMap = nodesMapRef;
        reconnectorTask = reconnectorTaskRef;
        initCtx = initCtxRef;
    }

    @Override
    public void initialize()
    {
        if (!nodesMap.isEmpty())
        {
            errorLog.logInfo("Reconnecting to previously known nodes");
            Collection<Node> nodes = nodesMap.values();
            reconnectorTask.startReconnecting(nodes, initCtx);
            errorLog.logInfo("Reconnect requests sent");
        }
        else
        {
            errorLog.logInfo("No known nodes.");
        }
    }

}
