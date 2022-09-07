package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

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

    public Flux<?> resourceConnected(Resource rsc, ResponseContext context)
    {
        ResourceDefinition rscDfn = rsc.getResourceDefinition();

        return Flux.merge(
            checkResourceDefinitionConnected(rsc.getResourceDefinition(), context),
            notifyListeners(
                "connecting to node '" + rsc.getNode().getName() + "' for resource '" + rscDfn.getName() + "'",
                connectionListener -> connectionListener.resourceConnected(rsc)
            )
        );
    }

    public Flux<?> checkResourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        boolean allOnline = true;
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
            while (rscIter.hasNext())
            {
                Resource rsc = rscIter.next();
                if (rsc.getNode().getPeer(apiCtx).getConnectionStatus() != ApiConsts.ConnectionStatus.ONLINE)
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

        return allOnline ?
            notifyListeners(
                "connecting to all nodes for resource '" + rscDfn.getName() + "'",
                connectionListener -> connectionListener.resourceDefinitionConnected(rscDfn, context)
            ) :
            Flux.empty();
    }

    private Flux<?> notifyListeners(
        String actionMessage,
        ConnectionListenerCallable callable
    )
    {
        List<Flux<ApiCallRc>> connectionListenerResponses = new ArrayList<>();

        for (CtrlSatelliteConnectionListener connectionListener : satelliteConnectionListeners)
        {
            try
            {
                connectionListenerResponses.addAll(
                    callable.call(connectionListener)
                );
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Error determining operations to perform after " + actionMessage
                );
            }
        }

        return Flux.fromIterable(connectionListenerResponses)
            .flatMap(listenerFlux -> listenerFlux.onErrorResume(exc -> handleListenerError(actionMessage, exc)));
    }

    private Publisher<? extends ApiCallRc> handleListenerError(
        String actionMessage,
        Throwable exc
    )
    {
        errorReporter.reportError(
            exc,
            null,
            null,
            "Error emitted performing operations after " + actionMessage
        );
        return Flux.empty();
    }

    @FunctionalInterface
    private interface ConnectionListenerCallable
    {
        Collection<Flux<ApiCallRc>> call(CtrlSatelliteConnectionListener connectionListener)
            throws AccessDeniedException;
    }
}
