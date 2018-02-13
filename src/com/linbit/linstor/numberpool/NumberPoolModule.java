package com.linbit.linstor.numberpool;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.ImplementationError;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Named;
import java.util.Iterator;

public class NumberPoolModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public MinorNrPool minorNrPool(
        @SystemContext AccessContext initCtx,
        @Named(CoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        CoreModule.ResourceDefinitionMap rscDfnMap
    )
    {
        MinorNrPool minorNrPool = new MinorNrPoolImpl(ctrlConfRef);

        try
        {
            minorNrPool.reloadRange();

            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                Iterator<VolumeDefinition> vlmIter = curRscDfn.iterateVolumeDfn(initCtx);
                while (vlmIter.hasNext())
                {
                    VolumeDefinition curVlmDfn = vlmIter.next();
                    MinorNumber minorNr = curVlmDfn.getMinorNr(initCtx);
                    minorNrPool.allocate(minorNr.value);
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

        return minorNrPool;
    }

    @Provides
    @Singleton
    public TcpPortPool tcpPortPool(
        @SystemContext AccessContext initCtx,
        @Named(CoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        CoreModule.ResourceDefinitionMap rscDfnMap
    )
    {
        TcpPortPool tcpPortPool = new TcpPortPoolImpl(ctrlConfRef);

        try
        {
            tcpPortPool.reloadRange();

            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                TcpPortNumber portNr = curRscDfn.getPort(initCtx);
                tcpPortPool.allocate(portNr.value);
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

        return tcpPortPool;
    }
}
