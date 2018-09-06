package com.linbit.linstor.numberpool;

import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Iterator;

import static com.linbit.linstor.numberpool.NumberPoolModule.MINOR_NUMBER_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.SF_TARGET_PORT_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.TCP_PORT_POOL;

public class DbNumberPoolInitializer
{
    private final ErrorReporter errorReporter;
    private final AccessContext initCtx;
    private final DynamicNumberPool minorNrPool;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool sfTargetPortPool;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.NodesMap nodesMap;

    @Inject
    public DbNumberPoolInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef,
        @Named(MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @Named(TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(SF_TARGET_PORT_POOL) DynamicNumberPool sfTargetPortPoolRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        errorReporter = errorReporterRef;
        initCtx = initCtxRef;
        minorNrPool = minorNrPoolRef;
        tcpPortPool = tcpPortPoolRef;
        sfTargetPortPool = sfTargetPortPoolRef;
        rscDfnMap = rscDfnMapRef;
        nodesMap = nodesMapRef;
    }

    public void initialize()
    {
        initializeMinorNrPool();
        initializeTcpPortPool();
        initializeSfTargetPortPool();
    }

    private void initializeMinorNrPool()
    {
        try
        {
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                Iterator<VolumeDefinition> vlmIter = curRscDfn.iterateVolumeDfn(initCtx);
                while (vlmIter.hasNext())
                {
                    VolumeDefinition curVlmDfn = vlmIter.next();
                    MinorNumber minorNr = curVlmDfn.getMinorNr(initCtx);
                    try
                    {
                        minorNrPool.allocate(minorNr.value);
                    }
                    catch (ValueInUseException exc)
                    {
                        errorReporter.logError(
                            "Skipping initial allocation in pool: " + exc.getMessage());
                    }
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "An " + accExc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                accExc
            );
        }
    }

    private void initializeTcpPortPool()
    {
        try
        {
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                TcpPortNumber portNr = curRscDfn.getPort(initCtx);
                try
                {
                    tcpPortPool.allocate(portNr.value);
                }
                catch (ValueInUseException exc)
                {
                    errorReporter.logError(
                        "Skipping initial allocation in pool: " + exc.getMessage());
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "An " + accExc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                accExc
            );
        }
    }

    private void initializeSfTargetPortPool()
    {
        try
        {
            for (Node curNode : nodesMap.values())
            {
                if (NodeType.SWORDFISH_TARGET.equals(curNode.getNodeType(initCtx)))
                {
                    try
                    {
                        Iterator<NetInterface> netIfIt = curNode.iterateNetInterfaces(initCtx);
                        int netIfCount = 0;
                        while (netIfIt.hasNext())
                        {
                            if (++netIfCount > 1)
                            {
                                throw new ImplementationError(
                                    "Swordfish target node has more than one network interface!"
                                );
                            }
                            NetInterface netIf = netIfIt.next();

                            sfTargetPortPool.allocate(netIf.getStltConnPort(initCtx).value);
                        }
                        if (netIfCount == 0)
                        {
                            throw new ImplementationError(
                                "Swordfish target node has no network interface!"
                            );
                        }
                    }
                    catch (ValueInUseException exc)
                    {
                        errorReporter.logError(
                            "Skipping initial allocation in pool: " + exc.getMessage());
                    }
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "An " + accExc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                accExc
            );
        }
    }
}
