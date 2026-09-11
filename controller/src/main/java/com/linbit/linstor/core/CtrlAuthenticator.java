package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.internal.IntAuthResponse;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerClosingConnectionException;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    private final IntAuthResponse intAuthResponse;
    private final PingTask pingTask;

    @Inject
    CtrlAuthenticator(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        SystemConfRepository systemConfRepositoryRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        IntAuthResponse intAuthResponseRef,
        PingTask pingTaskRef
    )
    {
        errorReporter = errorReporterRef;
        serializer = serializerRef;
        systemConfRepository = systemConfRepositoryRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        intAuthResponse = intAuthResponseRef;
        pingTask = pingTaskRef;
    }

    public void sendAuthentication(Peer peer)
    {
        completeAuthentication(peer.getNode())
            .contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME, InternalApiConsts.API_AUTH,
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
            .concatMap(
                peerWithResponse -> this.processAuthResponse(
                    peerWithResponse.objA,
                    peerWithResponse.objB
                )
            )
            .onErrorResume(
                PeerNotConnectedException.class,
                ignored -> Flux.empty()
            )
            .onErrorResume(
                PeerClosingConnectionException.class,
                ignored -> Flux.empty()
            );
    }

    private Flux<PairNonNull<Peer, ByteArrayInputStream>> completeAuthenticationInTransaction(Node node)
    {
        Flux<PairNonNull<Peer, ByteArrayInputStream>> flux;

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
            Peer peer = node.getPeer();
            if (peer instanceof TcpConnectorPeer tcpPeer)
            {
                errorReporter.logDebug("Adding peer to PingTask: '" + node.getName() + "'");
                pingTask.add(peer);
                flux = tcpPeer.apiCall(
                    InternalApiConsts.API_AUTH,
                    serializer
                        .headerlessBuilder()
                        .authMessage(
                            node.getUuid(),
                            node.getName().getDisplayName(),
                            "Hello, LinStor!".getBytes(StandardCharsets.UTF_8),
                            UUID.fromString(
                                systemConfRepository.getCtrlConfForView()
                                    .getProp(
                                        InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                                        ApiConsts.NAMESPC_CLUSTER
                                    )
                            )
                        )
                        .build(),
                    false,
                    false
                )
                    /*
                     * remember over which connection this Auth was actually sent. The response must be
                     * processed for exactly that connection - resolving node.getPeer() again when the response
                     * arrives could return a newer connection and would attribute the response (including its
                     * expectedFullSyncId) to the wrong connection.
                     */
                    .map(inputStream -> new PairNonNull<>(peer, inputStream));
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
        return flux;
    }

    private Flux<ApiCallRc> processAuthResponse(Peer peer, ByteArrayInputStream inputStream)
    {
        Flux<ApiCallRc> authResponseFlux;
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
                peer,
                "An IO exception occurred while parsing the authentication response"
            );
           authResponseFlux = Flux.empty();
        }
        return authResponseFlux;
    }
}
