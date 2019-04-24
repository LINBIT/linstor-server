package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlNodeCrtApiCallHandler
{
    private static final int FIRST_CONNECT_TIMEOUT_MILLIS = 1_000;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;
    private final Provider<CtrlAuthenticator> ctrlAuthenticator;
    private final ReconnectorTask reconnectorTask;


    @Inject
    public CtrlNodeCrtApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlNodeApiCallHandler ctrlNodeApiCallHandlerRef,
        Provider<CtrlAuthenticator> ctrlAuthenticatorRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
        ctrlAuthenticator = ctrlAuthenticatorRef;
        reconnectorTask = reconnectorTaskRef;
    }

    /**
     * Attempts to create a node by the given parameters. <br />
     * <br />
     * In any case an {@link ApiCallRc} is returned. The list of {@link ApiCallRcEntry}s describe the success
     * or failure of the operation. <br />
     * <br />
     * All return codes from this method are masked with {@link ApiConsts#MASK_NODE} and
     * {@link ApiConsts#MASK_CRT}.<br />
     * <br />
     * Following return codes can be returned:
     * <ul>
     *  <li>
     *      {@link ApiConsts#FAIL_ACC_DENIED_NODE} when the current access context does have enough privileges to
     *      change any nodes at all (controller.nodesMapLockProt)
     *  </li>
     *  <li>{@link ApiConsts#FAIL_MISSING_NETCOM} when the list of network interface apis is empty</li>
     *  <li>
     *      {@link ApiConsts#FAIL_INVLD_NET_NAME} when the list of network interface apis contains an invalid
     *      {@link NetInterfaceName}
     *  </li>
     *  <li>
     *      {@link ApiConsts#FAIL_INVLD_NET_ADDR} when the list of network interface apis contains an invalid
     *      {@link LsIpAddress}
     *  </li>
     *  <li>{@link ApiConsts#FAIL_MISSING_STLT_CONN} when the list of satellite connection apis is empty</li>
     *  <li>{@link ApiConsts#FAIL_INVLD_NODE_NAME} when the {@link NodeName} is invalid</li>
     *  <li>{@link ApiConsts#FAIL_INVLD_NODE_TYPE} when the {@link NodeType} is invalid</li>
     *  <li>{@link ApiConsts#CREATED} when the node was created successfully </li>
     * </ul>
     *
     * @param nodeNameStr
     * @param nodeTypeStr
     * @param netIfs
     * @param propsMap
     * @return
     */
    public Flux<ApiCallRc> createNode(
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        Map<String, String> propsMap
    )
    {
        ResponseContext context = CtrlNodeApiCallHandler.makeNodeContext(
            ApiOperation.makeCreateOperation(),
            nodeNameStr
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create node",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP),
            () -> createNodeInTransaction(context, nodeNameStr, nodeTypeStr, netIfs, propsMap)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }


    private Flux<ApiCallRc> createNodeInTransaction(
        ResponseContext context,
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        Map<String, String> propsMap
    )
    {
        Flux<ApiCallRc> flux;
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeData node;
        try
        {
            node = ctrlNodeApiCallHandler.createNodeImpl(
                nodeNameStr,
                nodeTypeStr,
                netIfs,
                propsMap,
                responses,
                context,
                false,
                true
            );
            flux = Flux.<ApiCallRc>just(responses)
                .concatWith(
                    ctrlSatelliteUpdateCaller.attemptConnecting(
                        peerAccCtx.get(),
                        node,
                        FIRST_CONNECT_TIMEOUT_MILLIS
                    )
                    .concatMap(connected -> processConnectingResponse(node, connected))
               );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getNodeDescriptionInline(nodeNameStr),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return flux;
    }

    private Flux<ApiCallRc> processConnectingResponse(NodeData node, boolean connected)
    {
        Flux<ApiCallRc> connectedFlux;
        if (connected)
        {
            connectedFlux = ctrlAuthenticator.get().completeAuthentication(node);
        }
        else
        {
            connectedFlux = Flux.just(
                ApiCallRcImpl.singletonApiCallRc(
                    ResponseUtils.makeNotConnectedWarning(
                        node.getName()
                    )
                )
            );
            try
            {
                reconnectorTask.add(node.getPeer(apiCtx), false);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return connectedFlux;
    }
}
