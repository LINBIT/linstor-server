package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@Singleton
public class CtrlSatelliteConnectionNotifier
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final Set<CtrlSatelliteConnectionListener> satelliteConnectionListeners;

    @Inject
    CtrlSatelliteConnectionNotifier(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        Set<CtrlSatelliteConnectionListener> satelliteConnectionListenersRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        satelliteConnectionListeners = satelliteConnectionListenersRef;
    }

    public Flux<?> checkResourceDefinitionConnected(ResourceDefinition rscDfn)
    {
        boolean allOnline = true;
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
            while (rscIter.hasNext())
            {
                Resource rsc = rscIter.next();
                if (rsc.getAssignedNode().getPeer(apiCtx).getConnectionStatus() != Peer.ConnectionStatus.ONLINE)
                {
                    allOnline = false;
                    break;
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(accExc);
        }

        Flux<?> flux = allOnline ?
            notifyResourceDefinitionConnected(rscDfn) :
            Flux.empty();

        return flux;
    }

    private Flux<?> notifyResourceDefinitionConnected(ResourceDefinition rscDfn)
    {
        ResourceName rscName = rscDfn.getName();

        List<Flux<ApiCallRc>> connectionListenerResponses = new ArrayList<>();

        for (CtrlSatelliteConnectionListener connectionListener : satelliteConnectionListeners)
        {
            try
            {
                connectionListenerResponses.addAll(
                    connectionListener.resourceDefinitionConnected(rscDfn)
                );
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Error determining operations to perform after connecting to all nodes for resource " +
                        "'" + rscName + "'"
                );
            }
        }

        return Flux.fromIterable(connectionListenerResponses)
            .flatMap(listenerFlux -> listenerFlux.onErrorResume(exc -> handleListenerError(rscName, exc)));
    }

    private Publisher<? extends ApiCallRc> handleListenerError(ResourceName rscName, Throwable exc)
    {
        errorReporter.reportError(
            exc,
            null,
            null,
            "Error emitted performing operations after connecting to all nodes for resource " + "'" + rscName + "'"
        );
        return Flux.empty();
    }
}
