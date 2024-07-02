package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.internal.IntAuthResponse;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerClosingConnectionException;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class CtrlAuthenticator
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer serializer;
    private final SystemConfRepository systemConfRepository;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext apiCtx;
    private final IntAuthResponse intAuthResponse;
    private final PingTask pingTask;

    @Inject
    CtrlAuthenticator(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        SystemConfRepository systemConfRepositoryRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext apiCtxRef,
        IntAuthResponse intAuthResponseRef,
        PingTask pingTaskRef
    )
    {
        errorReporter = errorReporterRef;
        serializer = serializerRef;
        systemConfRepository = systemConfRepositoryRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        apiCtx = apiCtxRef;
        intAuthResponse = intAuthResponseRef;
        pingTask = pingTaskRef;
    }

    public void sendAuthentication(Peer peer)
    {
        completeAuthentication(peer.getNode())
            .contextWrite(
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
            .concatMap(inputStream -> this.processAuthResponse(node, inputStream))
            .onErrorResume(
                PeerNotConnectedException.class,
                ignored -> Flux.empty()
            )
            .onErrorResume(
                PeerClosingConnectionException.class,
                ignored -> Flux.empty()
            );
    }

    private Flux<ByteArrayInputStream> completeAuthenticationInTransaction(Node node)
    {
        Flux<ByteArrayInputStream> flux;

        if (node.isDeleted())
        {
            errorReporter.logWarning(
                "Unable to complete authentication with peer '%s' because the node has been deleted", node);
            flux = Flux.error(new ImplementationError("complete authentication called on deleted peer"));
        }
        else
        {
            errorReporter.logInfo("Sending authentication to satellite '" + node.getName() + "'");
            // TODO make the shared secret customizable
            try
            {
                Peer peer = node.getPeer(peerAccCtx.get());
                if (peer instanceof TcpConnectorPeer)
                {
                    errorReporter.logDebug("Adding peer to PingTask: '" + node.getName() + "'");
                    pingTask.add(peer);
                    flux = ((TcpConnectorPeer) peer).apiCall(
                        InternalApiConsts.API_AUTH,
                        serializer
                            .headerlessBuilder()
                            .authMessage(
                                node.getUuid(),
                                node.getName().getDisplayName(),
                                "Hello, LinStor!".getBytes(),
                                UUID.fromString(
                                    systemConfRepository.getCtrlConfForView(apiCtx)
                                        .getProp(
                                            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                                            ApiConsts.NAMESPC_CLUSTER
                                        )
                                )
                            )
                            .build(),
                        false,
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

    private Flux<ApiCallRc> processAuthResponse(Node node, ByteArrayInputStream inputStream)
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
                "An IO exception occurred while parsing the authentication response"
            );
           authResponseFlux = Flux.empty();
        }
        return authResponseFlux;
    }

    private Peer getPeerPrivileged(Node node)
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
