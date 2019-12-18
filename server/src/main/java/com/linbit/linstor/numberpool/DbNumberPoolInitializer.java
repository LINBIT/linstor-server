package com.linbit.linstor.numberpool;

import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

import static com.linbit.linstor.numberpool.NumberPoolModule.LAYER_RSC_ID_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.MINOR_NUMBER_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.TCP_PORT_POOL;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.Iterator;

public class DbNumberPoolInitializer
{
    private final ErrorReporter errorReporter;
    private final AccessContext initCtx;
    private final DynamicNumberPool minorNrPool;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool layerRscIdPool;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.NodesMap nodesMap;

    @Inject
    public DbNumberPoolInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef,
        @Named(MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @Named(TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        errorReporter = errorReporterRef;
        initCtx = initCtxRef;
        minorNrPool = minorNrPoolRef;
        tcpPortPool = tcpPortPoolRef;
        layerRscIdPool = layerRscIdPoolRef;
        rscDfnMap = rscDfnMapRef;
        nodesMap = nodesMapRef;
    }

    public void initialize()
    {
        initializeMinorNrPool();
        initializeTcpPortPool();
        initializeLayerRscIdPool();
    }

    private void initializeMinorNrPool()
    {
        minorNrPool.reloadRange();
    }

    private void initializeTcpPortPool()
    {
        tcpPortPool.reloadRange();
    }

    private void initializeLayerRscIdPool()
    {
        layerRscIdPool.reloadRange();

        try
        {
            for (Node curNode : nodesMap.values())
            {
                Iterator<Resource> iterateResources = curNode.iterateResources(initCtx);
                while (iterateResources.hasNext())
                {
                    Resource rsc = iterateResources.next();
                    AbsRscLayerObject<?> rscLayerData = rsc.getLayerData(initCtx);

                    allocate(rscLayerData);
                }
                Iterator<Snapshot> iterateSnapshots = curNode.iterateSnapshots(initCtx);
                while (iterateSnapshots.hasNext())
                {
                    Snapshot snapshot = iterateSnapshots.next();
                    AbsRscLayerObject<?> rscLayerData = snapshot.getLayerData(initCtx);

                    allocate(rscLayerData);
                }

            }
        }
        catch (AccessDeniedException | ValueInUseException exc)
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
}
