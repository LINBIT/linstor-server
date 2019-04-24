package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.internal.IntAuthResponse;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

public class CtrlAuthenticator
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer serializer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext apiCtx;
    private final IntAuthResponse intAuthResponse;

    @Inject
    CtrlAuthenticator(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext apiCtxRef,
        IntAuthResponse intAuthResponseRef
    )
    {
        errorReporter = errorReporterRef;
        serializer = serializerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        apiCtx = apiCtxRef;
        intAuthResponse = intAuthResponseRef;
    }

    public void sendAuthentication(Peer peer)
    {
        completeAuthentication(peer.getNode())
            .subscriberContext(
                Context.of(
                    ApiModule.API_CALL_NAME, InternalApiConsts.API_AUTH,
                    AccessContext.class, peer.getAccessContext(),
                    Peer.class, peer
                )
            )
            .subscribe();
    }

    public Flux<ApiCallRc> completeAuthentication(Node node)
    {
        return scopeRunner.fluxInTransactionalScope(
            "authenticting node '" + node.getName() + "'",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP),
            () -> completeAuthenticationInTransaction(node)
        )
            .concatMap(inputStream -> this.processAuthResponse((NodeData) node, inputStream));
    }

    private Flux<ByteArrayInputStream> completeAuthenticationInTransaction(Node node)
    {
        Flux<ByteArrayInputStream> flux;

        if (node.isDeleted())
        {
            errorReporter.logWarning(
                "Unable to complete authentication with peer '%s' because the node has been deleted", node);
            flux = Flux.error(new ImplementationError("completeAuthenticateion called on deleted peer"));
        }
        else
        {
            errorReporter.logDebug("Sending authentication to satellite '" +
                node.getName() + "'");
            // TODO make the shared secret customizable
            try
            {
                Peer peer = node.getPeer(peerAccCtx.get());
                if (peer instanceof TcpConnectorPeer)
                {
                    flux = ((TcpConnectorPeer) peer).apiCall(
                        InternalApiConsts.API_AUTH,
                        serializer
                            .headerlessBuilder()
                            .authMessage(
                                node.getUuid(),
                                node.getName().getDisplayName(),
                                "Hello, LinStor!".getBytes()
                            )
                            .build(),
                        false
                    );

                }
                else
                {
                    flux = Flux.error(
                        new ImplementationError("Cannot authenticate against peer type " +
                            peer.getClass().getSimpleName()
                        )
                    );
                }
            }
            catch (AccessDeniedException exc)
            {
                flux = Flux.error(
                    new ApiAccessDeniedException(
                        exc,
                        "accessing '" + node.getName().displayValue + "' peer object.",
                        ApiConsts.FAIL_ACC_DENIED_NODE
                    )
                );
            }
        }
        return flux;
    }

    private Flux<ApiCallRc> processAuthResponse(NodeData node, ByteArrayInputStream inputStream)
    {
        Flux<ApiCallRc> authResponseFlux;
        Peer peer = getPeerPrivileged(node);
        try
        {
            authResponseFlux = intAuthResponse
                .executeReactive(
                    peer,
                    inputStream,
                    true
                );
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                ioExc,
                apiCtx,
                peer,
                "An IO exception occured while parsing the authentication response"
            );
           authResponseFlux = Flux.empty();
        }
        return authResponseFlux;
    }

    private Peer getPeerPrivileged(NodeData node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return peer;
    }
}
