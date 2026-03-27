package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgReqDrbdReactorExecOuterClass.DrbdReactorCommand;
import com.linbit.linstor.proto.responses.MsgRspDrbdReactorExecOuterClass.MsgRspDrbdReactorExec;
import com.linbit.linstor.security.AccessContext;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class CtrlExecNodeApiCallHandler
{
    private static final Pattern EVICT_TAKEOVER_PATTERN = Pattern.compile("Node '([^']+)' took over");

    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlStltSerializer stltComSerializer;
    private final NodeRepository nodeRepository;

    @Inject
    public CtrlExecNodeApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlStltSerializer stltComSerializerRef,
        NodeRepository nodeRepositoryRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        stltComSerializer = stltComSerializerRef;
        nodeRepository = nodeRepositoryRef;
    }

    public Flux<JsonGenTypes.ReactorExecResponse> nodeExecDrbdReactor(
        List<String> nodeNames,
        DrbdReactorCommand command,
        String config,
        boolean wait
    )
    {
        return scopeRunner.fluxInTransactionlessScope(
            "execute drbd-reactorctl",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
            () -> nodeExecDrbdReactorInScope(nodeNames, command, config, wait),
            MDC.getCopyOfContextMap()
        );
    }

    private Flux<JsonGenTypes.ReactorExecResponse> nodeExecDrbdReactorInScope(
        List<String> nodeNames,
        DrbdReactorCommand command,
        String config,
        boolean wait
    )
    {
        AccessContext peerCtx = peerAccCtx.get();
        byte[] msg = stltComSerializer.headerlessBuilder()
            .drbdReactorExecRequest(command, config, wait)
            .build();

        List<Flux<JsonGenTypes.ReactorExecResponse>> responses = new ArrayList<>();
        for (String nodeName : nodeNames)
        {
            JsonGenTypes.ReactorExecResponse resp = new JsonGenTypes.ReactorExecResponse();
            resp.node = nodeName;
            try
            {
                Node node = nodeRepository.get(peerCtx, LinstorParsingUtils.asNodeName(nodeName));
                if (node == null || node.isDeleted())
                {
                    resp.exit_code = -1;
                    resp.stderr_utf8 = "Node '" + nodeName + "' not found";
                    responses.add(Flux.just(resp));
                    continue;
                }

                Peer p = node.getPeer(peerCtx);
                if (p == null || !p.isOnline())
                {
                    resp.exit_code = -1;
                    resp.stderr_utf8 = "Node '" + nodeName + "' is offline";
                    responses.add(Flux.just(resp));
                    continue;
                }

                Flux<JsonGenTypes.ReactorExecResponse> nodeResponse = p.apiCall(InternalApiConsts.API_REQ_DRBD_REACTOR_EXEC, msg)
                    .map(bis -> deserializeReactorExecResponse(resp, bis, command, wait))
                    .switchIfEmpty(Mono.fromSupplier(() -> noResponseReactorExecResponse(resp, nodeName)))
                    .onErrorResume(exc -> Flux.just(communicationErrorReactorExecResponse(resp, exc)));
                responses.add(nodeResponse);
            }
            catch (Exception exc)
            {
                resp.exit_code = -1;
                resp.stderr_utf8 = "Error: " + exc.getMessage();
                responses.add(Flux.just(resp));
            }
        }

        return Flux.mergeSequential(responses);
    }

    private JsonGenTypes.ReactorExecResponse deserializeReactorExecResponse(
        JsonGenTypes.ReactorExecResponse resp,
        ByteArrayInputStream bis,
        DrbdReactorCommand command,
        boolean wait
    )
    {
        try
        {
            MsgRspDrbdReactorExec protoResp = ProtoDeserializationUtils.parseDrbdReactorExecResponse(bis);
            resp.exit_code = protoResp.getExitCode();
            resp.stdout_utf8 = protoResp.getStdout().toStringUtf8();
            resp.stderr_utf8 = protoResp.getStderr().toStringUtf8();
            if (command == DrbdReactorCommand.EVICT && wait)
            {
                resp.active_node = parseActiveNode(resp.stdout_utf8);
            }
        }
        catch (IOException exc)
        {
            resp.exit_code = -1;
            resp.stderr_utf8 = "Failed to parse response: " + exc.getMessage();
        }
        return resp;
    }

    private JsonGenTypes.ReactorExecResponse noResponseReactorExecResponse(
        JsonGenTypes.ReactorExecResponse resp,
        String nodeName
    )
    {
        resp.exit_code = -1;
        resp.stderr_utf8 = "No response received from node '" + nodeName + "'";
        return resp;
    }

    private JsonGenTypes.ReactorExecResponse communicationErrorReactorExecResponse(
        JsonGenTypes.ReactorExecResponse resp,
        Throwable exc
    )
    {
        resp.exit_code = -1;
        resp.stderr_utf8 = "Communication error: " + exc.getMessage();
        return resp;
    }

    private @Nullable String parseActiveNode(String output)
    {
        Matcher matcher = EVICT_TAKEOVER_PATTERN.matcher(output);
        return matcher.find() ? matcher.group(1) : null;
    }
}
