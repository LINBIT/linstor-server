package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.events.EventNodeHandlerBridge;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlNodeCrtApiCallHandler
{
    public static final int FIRST_CONNECT_TIMEOUT_MILLIS = 1_000;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;
    private final CtrlStorPoolCrtApiCallHandler ctrlStorPoolCrtHandler;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final Provider<CtrlAuthenticator> ctrlAuthenticator;
    private final ReconnectorTask reconnectorTask;
    private final CtrlRscAutoHelper autoHelper;
    private final EventNodeHandlerBridge eventNodeHandlerBridge;
    private final CtrlApiDataLoader dataLoader;

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
        CtrlStorPoolCrtApiCallHandler ctrlStorPoolCrtHandlerRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        Provider<CtrlAuthenticator> ctrlAuthenticatorRef,
        ReconnectorTask reconnectorTaskRef,
        CtrlRscAutoHelper autoHelperRef,
        EventNodeHandlerBridge eventNodeHandlerBridgeRef,
        CtrlApiDataLoader dataLoaderRef
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
        ctrlStorPoolCrtHandler = ctrlStorPoolCrtHandlerRef;
        rscDfnRepo = rscDfnRepoRef;
        ctrlAuthenticator = ctrlAuthenticatorRef;
        reconnectorTask = reconnectorTaskRef;
        autoHelper = autoHelperRef;
        eventNodeHandlerBridge = eventNodeHandlerBridgeRef;
        dataLoader = dataLoaderRef;
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

            if (nodeType.isSpecial())
            {
                node = ctrlNodeApiCallHandler.createSpecialSatellite(
                    nodeNameStr,
                    nodeTypeStr,
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

            eventNodeHandlerBridge.triggerNodeCreate(node.getApiData(apiCtx, null, null));

            flux = Flux.just(responses);
            flux = flux.concatWith(connectNow(context, node));
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

    public Flux<ApiCallRc> createEbsNode(
        String nodeNameStr,
        String ebsRemoteNameRef
    )
    {
        ResponseContext context = CtrlNodeApiCallHandler.makeNodeContext(
            ApiOperation.makeCreateOperation(),
            nodeNameStr
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create EBS node",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
            () -> createEbsNodeInTransaction(
                context,
                nodeNameStr,
                ebsRemoteNameRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createEbsNodeInTransaction(
        ResponseContext contextRef,
        String nodeNameStrRef,
        String ebsRemoteNameStrRef
    )
    {
        Flux<ApiCallRc> flux;
        ApiCallRcImpl responses = new ApiCallRcImpl();
        AbsRemote remote = dataLoader.loadRemote(ebsRemoteNameStrRef, true);
        if (!(remote instanceof EbsRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "The remote with the name '" + ebsRemoteNameStrRef +
                        "' is not a EBS remote."
                )
            );
        }
        Node node;
        try
        {
            node = ctrlNodeApiCallHandler.createSpecialSatellite(
                nodeNameStrRef,
                Node.Type.EBS_TARGET.name(),
                Collections.emptyMap()
            ).extractApiCallRc(responses);

            Flux<ApiCallRc> createStorPoolFlux = ctrlStorPoolCrtHandler.createStorPool(
                nodeNameStrRef,
                InternalApiConsts.EBS_DFTL_STOR_POOL_NAME,
                DeviceProviderKind.EBS_TARGET,
                null,
                false,
                Collections.singletonMap(
                    ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS + "/" +
                        ApiConsts.KEY_REMOTE,
                    ebsRemoteNameStrRef
                ),
                Flux.empty()
            );

            ctrlTransactionHelper.commit();

            eventNodeHandlerBridge.triggerNodeCreate(node.getApiData(apiCtx, null, null));

            flux = Flux.<ApiCallRc>just(responses)
                .log()
                .concatWith(connectNow(contextRef, node))
                .log()
                .concatWith(createStorPoolFlux);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getNodeDescriptionInline(nodeNameStrRef),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return flux;
    }

    public Flux<ApiCallRc> connectNow(ResponseContext context, Node node)
        throws AccessDeniedException
    {
        Flux<ApiCallRc> flux;
        Node.Type nodeType = node.getNodeType(apiCtx);

        if (!Node.Type.CONTROLLER.equals(nodeType) &&
            !Node.Type.AUXILIARY.equals(nodeType))
        {
            flux = ctrlSatelliteUpdateCaller.attemptConnecting(
                peerAccCtx.get(),
                node,
                FIRST_CONNECT_TIMEOUT_MILLIS
            )
                .concatMap(connected -> processConnectingResponse(node, connected, context));
        }
        else
        {
            errorReporter.logInfo(
                "Not connecting to node '%s' due to its node-type: %s",
                node.getName(),
                nodeType.name()
            );
            node.setOfflinePeer(errorReporter, apiCtx);
            flux = Flux.empty();
        }
        return flux;
    }

    private Flux<ApiCallRc> processConnectingResponse(Node node, boolean connected, ResponseContext context)
    {
        Flux<ApiCallRc> connectedFlux;
        if (connected)
        {
            connectedFlux = ctrlAuthenticator.get()
                .completeAuthentication(node)
                .concatWith(runAutoMagic(context));
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
                autoFluxes.add(autoHelper.manage(new AutoHelperContext(apiCallRcImpl, context, rscDfn)).getFlux());
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
        return Flux.<ApiCallRc>just(apiCallRcImpl)
            .concatWith(Flux.merge(autoFluxes));
    }
}
