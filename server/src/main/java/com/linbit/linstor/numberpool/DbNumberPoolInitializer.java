package com.linbit.linstor.numberpool;

import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.systemstarter.StartupInitializer;

import static com.linbit.linstor.numberpool.NumberPoolModule.LAYER_RSC_ID_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.MINOR_NUMBER_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.BACKUP_SHIPPING_PORT_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.Iterator;

public class DbNumberPoolInitializer implements StartupInitializer
{
    private final ErrorReporter errorReporter;
    private final DynamicNumberPool minorNrPool;
    private final DynamicNumberPool specStltTargetPortPool;
    private final DynamicNumberPool layerRscIdPool;
    private final CoreModule.NodesMap nodesMap;
    private final DynamicNumberPool backupShipPortPool;

    @Inject
    public DbNumberPoolInitializer(
        ErrorReporter errorReporterRef,
        @Named(MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @Named(SPECIAL_SATELLTE_PORT_POOL) DynamicNumberPool specStltTargetPortPoolRef,
        @Named(LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        @Named(BACKUP_SHIPPING_PORT_POOL) DynamicNumberPool backupShipPortPoolRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        errorReporter = errorReporterRef;
        minorNrPool = minorNrPoolRef;
        specStltTargetPortPool = specStltTargetPortPoolRef;
        layerRscIdPool = layerRscIdPoolRef;
        backupShipPortPool = backupShipPortPoolRef;
        nodesMap = nodesMapRef;
    }

    @Override
    public void initialize()
    {
        initializeMinorNrPool();
        initializeTcpPortPools();
        initializeSpecStltTargetPortPool();
        initializeLayerRscIdPool();
        initializeBackupShipPortPool();
    }

    private void initializeMinorNrPool()
    {
        minorNrPool.reloadRange();
    }

    private void initializeTcpPortPools()
    {
        for (Node node : nodesMap.values())
        {
            node.getTcpPortPool().reloadRange();
            allocateDrbdProxyPorts(node);
        }
    }

    // DrbdRscData ports get allocated during DB load, resource-connection proxy ports do not - do it here
    private void allocateDrbdProxyPorts(Node node)
    {
        DynamicNumberPool tcpPortPool = node.getTcpPortPool();
        Iterator<Resource> rscIt = node.iterateResources();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            for (ResourceConnection rscConn : rsc.getAbsResourceConnections())
            {
                @Nullable TcpPortNumber port;
                if (rsc.equals(rscConn.getSourceResource()))
                {
                    port = rscConn.getDrbdProxyPortSource();
                }
                else
                {
                    port = rscConn.getDrbdProxyPortTarget();
                }
                if (port != null)
                {
                    try
                    {
                        tcpPortPool.allocate(port.value);
                    }
                    catch (ValueInUseException exc)
                    {
                        errorReporter.logError(
                            "Skipping initial allocation in pool: " + exc.getMessage()
                        );
                    }
                }
            }
        }
    }

    private void initializeSpecStltTargetPortPool()
    {
        specStltTargetPortPool.reloadRange();
        for (Node curNode : nodesMap.values())
        {
            Node.Type nodeType = curNode.getNodeType();
            if (nodeType.isSpecial())
            {
                try
                {
                    Iterator<NetInterface> netIfIt = curNode.iterateNetInterfaces();
                    int netIfCount = 0;
                    while (netIfIt.hasNext())
                    {
                        if (++netIfCount > 1)
                        {
                            throw new ImplementationError(
                                "Special target node has more than one network interface!"
                            );
                        }
                        NetInterface netIf = netIfIt.next();
                        specStltTargetPortPool.allocate(netIf.getStltConnPort().value);
                    }
                    if (netIfCount == 0)
                    {
                        throw new ImplementationError(
                            "Special target node has no network interface!"
                        );
                    }
                }
                catch (ValueInUseException exc)
                {
                    errorReporter.logError(
                        "Skipping initial allocation in pool: " + exc.getMessage()
                    );
                }
            }
        }
    }

    private void initializeLayerRscIdPool()
    {
        layerRscIdPool.reloadRange();

        try
        {
            for (Node curNode : nodesMap.values())
            {
                Iterator<Resource> iterateResources = curNode.iterateResources();
                while (iterateResources.hasNext())
                {
                    Resource rsc = iterateResources.next();
                    AbsRscLayerObject<?> rscLayerData = rsc.getLayerData();

                    allocate(rscLayerData);
                }
                Iterator<Snapshot> iterateSnapshots = curNode.iterateSnapshots();
                while (iterateSnapshots.hasNext())
                {
                    Snapshot snapshot = iterateSnapshots.next();
                    AbsRscLayerObject<?> rscLayerData = snapshot.getLayerData();

                    allocate(rscLayerData);
                }

            }
        }
        catch (ValueInUseException exc)
        {
            throw new ImplementationError(
                "An " + exc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                    exc
                );
        }
    }

    private void allocate(AbsRscLayerObject<?> rscLayerDataRef) throws ValueInUseException
    {
        layerRscIdPool.allocate(rscLayerDataRef.getRscLayerId());
        for (AbsRscLayerObject<?> childRscLayerData : rscLayerDataRef.getChildren())
        {
            allocate(childRscLayerData);
        }
    }

    private void initializeBackupShipPortPool()
    {
        backupShipPortPool.reloadRange();
    }
}
