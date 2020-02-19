package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
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
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.types.LsIpAddress;
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

import java.util.ArrayList;
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
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final Provider<CtrlAuthenticator> ctrlAuthenticator;
    private final ReconnectorTask reconnectorTask;
    private final CtrlRscAutoHelper autoHelper;

    @Inject
    public CtrlNodeCrtApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlNodeApiCallHandler ctrlNodeApiCallHandlerRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        Provider<CtrlAuthenticator> ctrlAuthenticatorRef,
        ReconnectorTask reconnectorTaskRef,
        CtrlRscAutoHelper autoHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
        rscDfnRepo = rscDfnRepoRef;
        ctrlAuthenticator = ctrlAuthenticatorRef;
        reconnectorTask = reconnectorTaskRef;
        autoHelper = autoHelperRef;
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
     *  <li>{@link ApiConsts#FAIL_INVLD_NODE_TYPE} when the {@link Type} is invalid</li>
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

        Node node;
        try
        {
            Node.Type nodeType = LinstorParsingUtils.asNodeType(nodeTypeStr);

            if (Node.Type.OPENFLEX_TARGET.equals(nodeType))
            {
                node = ctrlNodeApiCallHandler.createOpenflexTargetNode(
                    nodeNameStr,
                    propsMap
                ).extractApiCallRc(responses);
            }
            else
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
            }

            flux = Flux.<ApiCallRc>just(responses);

            if (!Node.Type.CONTROLLER.equals(nodeType) &&
                !Node.Type.AUXILIARY.equals(nodeType))
            {
                flux = flux
                    .concatWith(
                        ctrlSatelliteUpdateCaller.attemptConnecting(
                            peerAccCtx.get(),
                            node,
                            FIRST_CONNECT_TIMEOUT_MILLIS
                        )
                        .concatMap(connected -> processConnectingResponse(node, connected))
                    )
                    .concatWith(runAutoMagic(context));
            }
            else
            {
                node.setOfflinePeer(apiCtx);
            }
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

    private Flux<ApiCallRc> processConnectingResponse(Node node, boolean connected)
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

    private Flux<ApiCallRc> runAutoMagic(ResponseContext context)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Auto-Quorum and -Tiebreaker after node create",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
            () -> runAutoMagicInTransaction(context)
        );
    }

    private Flux<ApiCallRc> runAutoMagicInTransaction(ResponseContext context)
    {
        ApiCallRcImpl apiCallRcImpl = new ApiCallRcImpl();
        List<Flux<ApiCallRc>> autoFluxes = new ArrayList<>();
        try
        {
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(peerAccCtx.get()).values())
            {
                autoFluxes.add(autoHelper.manage(apiCallRcImpl, context, rscDfn).getFlux());
            }

            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Running auto-quorum and -tiebreaker on new node",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return Flux.<ApiCallRc> just(apiCallRcImpl)
            .concatWith(Flux.merge(autoFluxes));
    }
}
