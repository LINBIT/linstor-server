package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.ResourceWithPayloadPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.events.EventNodeHandlerBridge;
import com.linbit.linstor.backupshipping.BackupConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.PortAlreadyInUseException;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.CopySnapsHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupQueueInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.SatelliteResourceStateDrbdUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NetInterfaceApi.StltConn;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.SatelliteConfigApi;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeControllerFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.core.utils.ResourceUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyConfigResponseOuterClass.MsgIntApplyConfigResponse;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.CollectionUtils;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.api.ApiConsts.DEFAULT_NETIF;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater.findNodesToContact;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;

@Singleton
public class CtrlNodeApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeControllerFactory nodeFactory;
    private final NetInterfaceFactory netInterfaceFactory;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final SatelliteConnector satelliteConnector;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final StorPoolHelper storPoolHelper;
    private final DynamicNumberPool specStltPortPool;
    private final SpecialSatelliteProcessManager specStltProcMgr;
    private final ReconnectorTask reconnectorTask;
    private final Scheduler scheduler;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlStltSerializer stltComSerializer;
    private final AutoDiskfulTask autoDiskfulTask;
    private final CtrlRscAutoRePlaceRscHelper autoRePlaceRscHelper;
    private final ErrorReporter errorReporter;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final CtrlRscDeleteApiCallHandler rscDeleteHandler;
    private final CtrlSnapshotDeleteApiCallHandler snapDeleteHandler;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskApiCallHandler;
    private final Autoplacer autoplacer;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final EventNodeHandlerBridge eventNodeHandlerBridge;
    private final Provider<CtrlNodeCrtApiCallHandler> ctrlNodeCrtApiCallHandlerProvider;
    private final CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandler;
    private final CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandler;
    private final CtrlRscAutoHelper ctrlRscAutoHelper;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderProvider;
    private final CopySnapsHelper copySnapsHelper;

    @Inject
    public CtrlNodeApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeControllerFactory nodeFactoryRef,
        NetInterfaceFactory netInterfaceFactoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        SatelliteConnector satelliteConnectorRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        StorPoolHelper storPoolHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL) DynamicNumberPool specStltTargetPortPoolRef,
        SpecialSatelliteProcessManager specStltTargetProcMgrRef,
        ReconnectorTask reconnectorTaskRef,
        Scheduler schedulerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlStltSerializer stltComSerializerRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        CtrlRscAutoRePlaceRscHelper autoRePlaceRscHelperRef,
        ErrorReporter errorReporterRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlRscDeleteApiCallHandler rscDeleteHandlerRef,
        CtrlSnapshotDeleteApiCallHandler snapDeleteHandlerRef,
        Autoplacer autoplacerRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskApiCallHandlerRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        EventNodeHandlerBridge eventNodeHandlerBridgeRef,
        Provider<CtrlNodeCrtApiCallHandler> ctrlNodeCrtApiCallHandlerProviderRef,
        CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandlerRef,
        CtrlBackupQueueInternalCallHandler ctrlBackupQueueHandlerRef,
        CtrlRscAutoHelper ctrlRscAutoHelperRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderProviderRef,
        CopySnapsHelper copySnapsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeFactory = nodeFactoryRef;
        netInterfaceFactory = netInterfaceFactoryRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        satelliteConnector = satelliteConnectorRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        storPoolHelper = storPoolHelperRef;
        peerAccCtx = peerAccCtxRef;
        specStltPortPool = specStltTargetPortPoolRef;
        specStltProcMgr = specStltTargetProcMgrRef;
        reconnectorTask = reconnectorTaskRef;
        scheduler = schedulerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltComSerializer = stltComSerializerRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        autoRePlaceRscHelper = autoRePlaceRscHelperRef;
        errorReporter = errorReporterRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        rscDeleteHandler = rscDeleteHandlerRef;
        snapDeleteHandler = snapDeleteHandlerRef;
        autoplacer = autoplacerRef;
        rscToggleDiskApiCallHandler = rscToggleDiskApiCallHandlerRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        eventNodeHandlerBridge = eventNodeHandlerBridgeRef;
        ctrlNodeCrtApiCallHandlerProvider = ctrlNodeCrtApiCallHandlerProviderRef;
        ctrlBackupCrtApiCallHandler = ctrlBackupCrtApiCallHandlerRef;
        ctrlBackupQueueHandler = ctrlBackupQueueHandlerRef;
        ctrlRscAutoHelper = ctrlRscAutoHelperRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        propsChangeListenerBuilderProvider = propsChangeListenerBuilderProviderRef;
        copySnapsHelper = copySnapsHelperRef;
    }

    Node createNodeImpl(
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        Map<String, String> propsMap,
        ApiCallRcImpl responses,
        ResponseContext context,
        boolean startConnecting,
        boolean autoCommit
    )
        throws AccessDeniedException
    {
        requireNodesMapChangeAccess();
        Node node = null;
        if (netIfs.isEmpty())
        {
            // TODO for auxiliary nodes maybe no netif required?
            reportMissingNetInterfaces(nodeNameStr); // throws exception
        }
        else
        {
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);

            Node.Type type = LinstorParsingUtils.asNodeType(nodeTypeStr);

            node = createNode(nodeName, type);

            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.NODE,
                propsMap,
                ctrlPropsHelper.getProps(node),
                ApiConsts.FAIL_ACC_DENIED_NODE,
                prefixesIgnoringWhitelistCheck
            );

            for (NetInterfaceApi netIfApi : netIfs)
            {
                TcpPortNumber port = null;
                EncryptionType encrType = null;
                StltConn stltConn = netIfApi.getStltConn();
                if (stltConn != null)
                {
                    port = LinstorParsingUtils.asTcpPortNumber(stltConn.getSatelliteConnectionPort());
                    encrType = LinstorParsingUtils.asEncryptionType(stltConn.getSatelliteConnectionEncryptionType());
                }

                NetInterface netIf = createNetInterface(
                    node,
                    LinstorParsingUtils.asNetInterfaceName(netIfApi.getName()),
                    LinstorParsingUtils.asLsIpAddress(netIfApi.getAddress()),
                    port,
                    encrType
                );

                if (stltConn != null && getActiveStltConn(node) == null)
                {
                    setActiveStltConn(node, netIf);
                }

                node.addNetInterface(peerAccCtx.get(), netIf);
            }

            if (getActiveStltConn(node) == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NO_STLT_CONN_DEFINED,
                    "No satellite connection defined for " + getNodeDescriptionInline(nodeNameStr)
                ));
            }
            else
            {
                nodeRepository.put(apiCtx, nodeName, node);

                if (type.isDeviceProviderKindAllowed(DeviceProviderKind.DISKLESS))
                {
                    // create default diskless storage pool
                    // this has to happen AFTER we added the node into the nodeRepository
                    // otherwise createStorPool will not find the node by its nodeNameStr
                    storPoolHelper.createStorPool(
                        nodeNameStr,
                        LinStor.DISKLESS_STOR_POOL_NAME,
                        DeviceProviderKind.DISKLESS,
                        (String) null,
                        false // no diskless shared SP
                    );
                }

                if (autoCommit)
                {
                    ctrlTransactionHelper.commit();
                }

                errorReporter.logInfo("Node created %s/%s/%s",
                    nodeNameStr, netIfs.get(0).getAddress(), nodeTypeStr);

                responseConverter.addWithOp(responses, context,
                    ApiSuccessUtils.defaultRegisteredEntry(node.getUuid(), getNodeDescriptionInline(node)));

                if (startConnecting)
                {
                    satelliteConnector.startConnecting(node, peerAccCtx.get());
                }
            }
        }
        return node;
    }

    public ApiCallRcWith<Node> createSpecialSatellite(
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> propsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Node node = null;
        ResponseContext context = makeNodeContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr
        );

        int specStltPort = 0;

        boolean retry = true;
        while (retry)
        {
            try
            {
                specStltPort = specStltPortPool.autoAllocate();

                // throws PortAlreadyInUseException
                specStltProcMgr.startLocalSatelliteProcess(
                    nodeNameStr,
                    specStltPort,
                    Node.Type.valueOfIgnoreCase(nodeTypeStr, null)
                );

                retry = false;
            }
            catch (PortAlreadyInUseException exc)
            {
                // ignored, try the next port
            }
            catch (ExhaustedPoolException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_POOL_EXHAUSTED_SPECIAL_SATELLTE_TCP_PORT,
                        "No TCP/IP port number could be allocated for the " + nodeTypeStr + " node"
                    ),
                    exc
                );
            }
            catch (IOException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR,
                        nodeNameStr
                    ),
                    exc
                );
            }
        }
        try
        {
            List<NetInterfaceApi> netIfs = new ArrayList<>();
            netIfs.add(
                new NetInterfacePojo(
                    UUID.randomUUID(),
                    DEFAULT_NETIF,
                    "127.0.0.1",
                    specStltPort,
                    ApiConsts.VAL_NETCOM_TYPE_PLAIN
                )
            );
            node = createNodeImpl(
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
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }
        return new ApiCallRcWith<>(responses, node);
    }

    private void setActiveStltConn(Node node, NetInterface netIf)
    {
        try
        {
            node.setActiveStltConn(peerAccCtx.get(), netIf);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "set the current satellite connection of " + getNodeDescriptionInline(node),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private @Nullable NetInterface getActiveStltConn(Node node)
    {
        NetInterface netIf;
        try
        {
            netIf = node.getActiveStltConn(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access the current satellite connection of " + getNodeDescriptionInline(node),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netIf;
    }

    public Flux<ApiCallRc> modify(
        @Nullable UUID nodeUuid,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ResponseContext context = makeNodeContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify node",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP),
                () -> modifyInTransaction(
                    nodeUuid,
                    nodeNameStr,
                    nodeTypeStr,
                    overrideProps,
                    deletePropKeys,
                    deleteNamespaces,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        @Nullable UUID nodeUuid,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts = false;

        List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
            Node node = ctrlApiDataLoader.loadNode(nodeName, true);
            if (nodeUuid != null && !nodeUuid.equals(node.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_NODE,
                    "UUID-check failed"
                ));
            }
            if (nodeTypeStr != null)
            {
                boolean needsReconnect = setNodeType(node, nodeTypeStr);
                if (needsReconnect)
                {
                    flux = flux.concatWith(reconnectNode(Arrays.asList(node.getName().displayValue)));
                }
                notifyStlts = true;
            }

            Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilderProvider.get()
                .buildPropsChangedListeners(peerAccCtx.get(), node, specialPropFluxes);

            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            Props props = ctrlPropsHelper.getProps(node);
            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.NODE,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_NODE,
                prefixesIgnoringWhitelistCheck,
                propsChangedListeners
            ) || notifyStlts;
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs,
                LinStorObject.NODE,
                props,
                deletePropKeys,
                deleteNamespaces,
                prefixesIgnoringWhitelistCheck,
                propsChangedListeners
            ) || notifyStlts;

            flux = flux.concatWith(checkProperties(apiCallRcs, node, overrideProps, deletePropKeys, deleteNamespaces));

            NodeApi oldNodeData = node.getApiData(peerAccCtx.get(), null, null);

            ctrlTransactionHelper.commit();

            eventNodeHandlerBridge.triggerNodeModified(oldNodeData,
                node.getApiData(peerAccCtx.get(), null, null));

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                node.getUuid(), getNodeDescriptionInline(node)));

            if (notifyStlts)
            {
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        nodeName,
                        findNodesToContact(apiCtx, node)
                    ).flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(flux)
            .concatWith(Flux.merge(specialPropFluxes));
    }

    public Flux<ApiCallRc> reconnectNode(
        List<String> nodes
    )
    {
        ResponseContext context = makeNodeContext(
            ApiOperation.makeModifyOperation(),
            StringUtils.join(nodes, ", ")
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Reconnect node(s)",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP),
                () -> reconnectNodesInTransaction(
                    nodes
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> reconnectNodesInTransaction(Collection<String> nodes)
    {
        Flux<ApiCallRc> waitForConnectFlux = Flux.empty();
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            ApiOperation.makeModifyOperation(),
            "Nodes: [" + String.join(",", nodes) + "]",
            "nodes: [" + String.join(",", nodes) + "]",
            ApiConsts.MASK_NODE,
            new HashMap<>()
        );
        List<String> reconNodes = new ArrayList<>();
        List<String> evictNodes = new ArrayList<>();

        try
        {
            for (String nodeStr : nodes)
            {
                Node node = ctrlApiDataLoader.loadNode(new NodeName(nodeStr), true);
                node.getPeer(apiCtx); // check for access

                if (!node.getFlags().isSet(apiCtx, Node.Flags.EVICTED))
                {
                    node.getPeer(apiCtx).closeConnection(false);
                    waitForConnectFlux = waitForConnectFlux
                        .concatWith(ctrlNodeCrtApiCallHandlerProvider.get().connectNow(node));
                    reconNodes.add(nodeStr);
                }
                else
                {
                    evictNodes.add(nodeStr);
                }
            }

            if (!reconNodes.isEmpty())
            {
                responses.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_MOD | ApiConsts.MASK_NODE | ApiConsts.MASK_SUCCESS,
                        "Nodes [" + String.join(",", reconNodes) + "] will be reconnected."
                    )
                );
            }
            if (!evictNodes.isEmpty())
            {
                responses.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_MOD | ApiConsts.MASK_NODE | ApiConsts.WARN_NODE_EVICTED,
                        "Nodes [" + String.join(",", evictNodes) + "] are evicted and will not be reconnected. " +
                        "Use node restore <node-name> to reconnect."
                    )
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }
        return Flux.<ApiCallRc>just(responses).concatWith(waitForConnectFlux);
    }

    ArrayList<NodeApi> listNodes(List<String> nodeNames, List<String> propFilters)
    {
        ArrayList<NodeApi> nodes = new ArrayList<>();
        final Set<NodeName> nodesFilter =
            nodeNames.stream().map(LinstorParsingUtils::asNodeName).collect(Collectors.toSet());

        try
        {
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(node ->
                    (
                        nodesFilter.isEmpty() ||
                        nodesFilter.contains(node.getName())
                    )
                )
                .forEach(node ->
                    {
                        try
                        {
                            final ReadOnlyProps props = node.getProps(peerAccCtx.get());
                            if (props.contains(propFilters))
                            {
                                nodes.add(node.getApiData(peerAccCtx.get(), null, null));
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add node without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return nodes;
    }

    private Node createNode(NodeName nodeName, Node.Type type)
    {
        Node node;
        try
        {
            node = nodeFactory.create(
                peerAccCtx.get(),
                nodeName,
                type,
                new Node.Flags[0]
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // accDeniedExc during creation means that an objectProtection already exists
            // and gives no permission to the accCtx to access it.
            // This means we have an existing objProt without corresponding Node --> exception
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_ACC_DENIED_NODE,
                    "ObjProt of non-existing Node denies access of registering the Node in question."
                ),
                new LinStorException(
                    "An accessDeniedException occurred during creation of a node. That means the " +
                        "ObjectProtection (of the non-existing Node) denied access to the node. " +
                        "It is possible that someone has modified the database accordingly. Please " +
                        "file a bug report otherwise.",
                    accDeniedExc
                ));
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_EXISTS_NODE,
                    "Registration of node '" + nodeName.displayValue + "' failed."
                )
                .setCause("A node with the specified name '" + nodeName.displayValue + "' already exists.")
                .setCorrection("- Specify another name for the new node\n" +
                    "or\n" +
                    "- Delete the existing node before creating a new node with the same name")
                .setSkipErrorReport(true)
                .build(),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return node;
    }

    private void reportMissingNetInterfaces(String nodeNameStr)
    {
        throw new ApiRcException(ApiCallRcImpl
            .entryBuilder(ApiConsts.FAIL_MISSING_NETCOM, "Registration of node '" + nodeNameStr + "' failed.")
            .setCause("No network interfaces were given.")
            .setCorrection("At least one network interface has to be given and be marked to be used for " +
            "controller-satellite communitaction.")
            .build()
        );
    }

    private NetInterface createNetInterface(
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        @Nullable TcpPortNumber port,
        @Nullable EncryptionType type
    )
    {
        NetInterface netIf;
        try
        {
            netIf = netInterfaceFactory.create(
                peerAccCtx.get(),
                node,
                netName,
                addr,
                port,
                type
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the netinterface '" + netName + "' on node '" + node.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_EXISTS_NET_IF, "Registration of node '" + node.getName() + "' failed.")
                .setCause("A duplicate network interface name was encountered during node registration.")
                .setDetails("The network interface name '" + netName +
                    "' was specified for more than one network interface.")
                .setCorrection("A name that is unique per node must be specified for each network interface.")
                .setSkipErrorReport(true)
                .build(),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return netIf;
    }

    private boolean setNodeType(Node node, String nodeTypeStr)
    {
        boolean needsReconnect = false;

        Node.Type nodeType = LinstorParsingUtils.asNodeType(nodeTypeStr);
        try
        {
            Node.Type oldType = node.getNodeType(apiCtx);

            boolean allowed = true;
            if (oldType.isSpecial() != nodeType.isSpecial())
            {
                allowed = false;
            }

            if (!allowed)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_NODE_TYPE,
                        "Failed to change node type"
                    ).setCause(
                        "Changing node types from " + oldType.name() + " to " + nodeType.name() + " is not allowed"
                    ).build()
                );
            }

            if (!node.streamStorPools(apiCtx)
                .map(StorPool::getDeviceProviderKind)
                .allMatch(nodeType::isDeviceProviderKindAllowed)
            )
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_STOR_DRIVER,
                        "Failed to change node type"
                    )
                    .setCause("The current node has at least one storage pool with a storage driver " +
                        "that is not compatible with node type '" + nodeTypeStr + "'")
                    .build()
                );
            }
            if (nodeType.isSpecial() && node.streamNetInterfaces(apiCtx).count() != 1)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_NODE_TYPE,
                        "Failed to change node type"
                    )
                        .setCause(
                            "A node with type " + nodeType.name() + " is only allowed to have 1 network interface"
                        )
                        .build()
                );
            }

            NetInterface activeStltConn = node.getActiveStltConn(apiCtx);
            EncryptionType stltConnEncryptionType = activeStltConn.getStltConnEncryptionType(apiCtx);
            int currentStltPort = activeStltConn.getStltConnPort(apiCtx).value;

            int newStltPort = currentStltPort;
            EncryptionType encrType = null;

            if (oldType.equals(Node.Type.CONTROLLER) &&
                (nodeType.equals(Node.Type.SATELLITE) || nodeType.equals(Node.Type.COMBINED)))
            {
                switch (stltConnEncryptionType)
                {
                    case PLAIN:
                        encrType = EncryptionType.PLAIN;
                        newStltPort = ApiConsts.DFLT_STLT_PORT_PLAIN;
                        break;
                    case SSL:
                        encrType = EncryptionType.SSL;
                        newStltPort = ApiConsts.DFLT_STLT_PORT_SSL;
                        break;
                    default:
                        throw new ImplementationError("Unexpected encryption type: " + stltConnEncryptionType);
                }
            }
            else if ((oldType.equals(Node.Type.SATELLITE) || oldType.equals(Node.Type.COMBINED)) &&
                nodeType.equals(Node.Type.CONTROLLER))
            {
                switch (stltConnEncryptionType)
                {
                    case PLAIN:
                        encrType = EncryptionType.PLAIN;
                        newStltPort = ApiConsts.DFLT_CTRL_PORT_PLAIN;
                        break;
                    case SSL:
                        encrType = EncryptionType.SSL;
                        newStltPort = ApiConsts.DFLT_CTRL_PORT_SSL;
                        break;
                    default:
                        throw new ImplementationError("Unexpected encryption type: " + stltConnEncryptionType);
                }
            }
            if (encrType != null)
            {
                try
                {
                    activeStltConn.setStltConn(apiCtx, new TcpPortNumber(newStltPort), encrType);
                    needsReconnect = true;
                }
                catch (ValueOutOfRangeException exc)
                {
                    throw new ImplementationError(exc);
                }
                // update satellite port
            }

            node.setNodeType(peerAccCtx.get(), nodeType);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update the node type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return needsReconnect;
    }

    private void requireNodesMapChangeAccess()
    {
        try
        {
            nodeRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any nodes",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    /**
     * Checks for sanity of the currently set properties. There are checks that throw Exceptions if they are not passed,
     * other checks only generate a warning message for the user.
     *
     * @param apiCallRcsRef
     * @param deleteNamespacesRef
     * @param overridePropsRef
     *
     * @throws InvalidNameException
     * @throws AccessDeniedException
     */
    private Flux<ApiCallRc> checkProperties(
        ApiCallRcImpl apiCallRcsRef,
        Node node,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
        throws AccessDeniedException, InvalidNameException
    {
        Flux<ApiCallRc> retFlux = Flux.empty();

        /*
         * Checks that throw exceptions
         */

        // check if specified preferred network interface exists
        ctrlPropsHelper.checkPrefNic(
            apiCtx,
            node,
            overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
            ApiConsts.MASK_NODE
        );

        /*
         * Checks that only generate warnings
         */
        ExtToolsInfo drbd9 = node.getPeer(apiCtx).getExtToolsManager().getExtToolInfo(ExtTools.DRBD9_KERNEL);
        ExtToolsInfo drbdProxy = node.getPeer(apiCtx).getExtToolsManager().getExtToolInfo(ExtTools.DRBD_PROXY);
        boolean isDrbd9Supported = drbd9 != null && drbd9.isSupported();
        boolean isDrbdProxySupported = drbdProxy != null && drbdProxy.isSupported();
        for (Entry<String, String> entry : overrideProps.entrySet())
        {
            if (entry.getKey().startsWith("Drbd"))
            {
                if (entry.getKey().startsWith("DrbdProxy"))
                {
                    if (!isDrbdProxySupported)
                    {
                        apiCallRcsRef.addEntry(
                            "The property '" + entry.getKey() + "' has no effect since the node '" +
                                node.getName().displayValue + "' does not support DRBD_PROXY",
                            ApiConsts.WARN_UNEFFECTIVE_PROP
                        );
                    }
                }
                else
                {
                    if (!isDrbd9Supported)
                    {
                        apiCallRcsRef.addEntry(
                            "The property '" + entry.getKey() + "' has no effect since the node '" +
                                node.getName().displayValue + "' does not support DRBD 9",
                            ApiConsts.WARN_UNEFFECTIVE_PROP
                        );
                    }
                }
            }
        }

        String autoDiskfulKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_DISKFUL;
        if (
            overrideProps.containsKey(autoDiskfulKey) || deletePropKeys.contains(autoDiskfulKey) ||
                deleteNamespaces.contains(ApiConsts.NAMESPC_DRBD_OPTIONS)
        )
        {
            autoDiskfulTask.update(node);
        }

        boolean hasKeyInDrbdOptions = false;
        boolean maxConcurrentShippingsChanged = false;
        for (String key : overrideProps.keySet())
        {
            if (key.startsWith(ApiConsts.NAMESPC_DRBD_OPTIONS))
            {
                hasKeyInDrbdOptions = true;
            }
            else if (key.equals(BackupConsts.CONCURRENT_BACKUPS_KEY))
            {
                maxConcurrentShippingsChanged = true;
            }
        }
        for (String key : deletePropKeys)
        {
            if (key.startsWith(ApiConsts.NAMESPC_DRBD_OPTIONS))
            {
                hasKeyInDrbdOptions = true;
            }
            else if (key.equals(BackupConsts.CONCURRENT_BACKUPS_KEY))
            {
                maxConcurrentShippingsChanged = true;
            }
        }
        hasKeyInDrbdOptions |= deleteNamespaces.contains(ApiConsts.NAMESPC_DRBD_OPTIONS);
        if (hasKeyInDrbdOptions)
        {
            for (PairNonNull<Flux<ApiCallRc>, Peer> pair : reconnectorTask.rerunConfigChecks())
            {
                retFlux = retFlux.concatWith(pair.objA);
            }
        }
        if (maxConcurrentShippingsChanged)
        {
            retFlux = retFlux.concatWith(ctrlBackupQueueHandler.maxConcurrentShippingsChangedForNode(node));
        }

        return retFlux;
    }

    public static String getNodeDescription(String nodeNameStr)
    {
        return "Node: " + nodeNameStr;
    }

    public static String getNodeDescriptionInline(Node node)
    {
        return getNodeDescriptionInline(node.getName().displayValue);
    }

    public static String getNodeDescriptionInline(String nodeNameStr)
    {
        return "node '" + nodeNameStr + "'";
    }

    public static ResponseContext makeNodeContext(
        ApiOperation operation,
        String nodeNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);

        return new ResponseContext(
            operation,
            getNodeDescription(nodeNameStr),
            getNodeDescriptionInline(nodeNameStr),
            ApiConsts.MASK_NODE,
            objRefs
        );
    }

    public StltConfig getConfig(String nodeName) throws AccessDeniedException
    {
        return ctrlApiDataLoader.loadNode(nodeName, true).getPeer(peerAccCtx.get()).getStltConfig();
    }

    public Flux<ApiCallRc> setGlobalConfig(SatelliteConfigApi config) throws AccessDeniedException
    {
        ArrayList<Flux<ApiCallRc>> answers = new ArrayList<>();

        for (NodeName nodeName : nodeRepository.getMapForView(peerAccCtx.get()).keySet())
        {
            answers.add(setConfig(nodeName.getName(), config));
        }
        ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
            ApiConsts.MODIFIED | ApiConsts.MASK_CTRL_CONF,
            "Successfully updated controller config"
        );
        answers.add(Flux.just(rc));
        return Flux.merge(answers);
    }

    public Flux<ApiCallRc> setConfig(String nodeName, SatelliteConfigApi config)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "set satellite config",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP),
            () -> setStltConfig(nodeName, config),
            MDC.getCopyOfContextMap()
        );
    }

    private Flux<ApiCallRc> setStltConfig(String nodeName, SatelliteConfigApi config)
        throws IOException, AccessDeniedException
    {
        Flux<ApiCallRc> flux;
        Peer curPeer = ctrlApiDataLoader.loadNode(nodeName, true).getPeer(peerAccCtx.get());
        if (!curPeer.isOnline())
        {
            flux = Flux.empty();
        }
        else
        {
            StltConfig stltConf = curPeer.getStltConfig();
            String logLevel = config.getLogLevel();
            String logLevelLinstor = config.getLogLevelLinstor();
            if (logLevel == null || logLevel.isEmpty())
            {
                if (((logLevelLinstor != null) && !logLevelLinstor.isEmpty()))
                {
                    LinstorParsingUtils.asLogLevel(logLevelLinstor);
                    stltConf.setLogLevelLinstor(logLevelLinstor);
                }
            }
            else
            {
                LinstorParsingUtils.asLogLevel(logLevel);
                stltConf.setLogLevel(logLevel);
                if (((logLevelLinstor != null) && !logLevelLinstor.isEmpty()))
                {
                    LinstorParsingUtils.asLogLevel(logLevelLinstor);
                    stltConf.setLogLevelLinstor(logLevelLinstor);
                }
            }
            ResponseContext context = makeNodeContext(ApiOperation.makeModifyOperation(), nodeName);
            byte[] msg = stltComSerializer.headerlessBuilder().changedConfig(stltConf).build();
            flux = curPeer
                .apiCall(InternalApiConsts.API_MOD_STLT_CONFIG, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
                .map(
                    responseMsg ->
                    {
                        MsgIntApplyConfigResponse resp;
                        ApiCallRc rc;
                        try
                        {
                            resp = MsgIntApplyConfigResponse.parseDelimitedFrom(responseMsg);
                            if (resp.getSuccess())
                            {
                                rc = ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.MODIFIED | ApiConsts.MASK_NODE,
                                    "Successfully updated satellite config"
                                );
                            }
                            else
                            {
                                rc = ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_NODE,
                                    "Failure while updating satellite config"
                                );
                            }
                        }
                        catch (IOException exc)
                        {
                            rc = ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_NODE,
                                "Failure while updating satellite config"
                            );
                        }
                        return rc;
                    }
                )
                .transform(response -> responseConverter.reportingExceptions(context, response));
        }
        return flux;
    }

    public Flux<ApiCallRc> restoreNode(String nodeName, boolean deleteResources, boolean deleteSnapshots)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Restore node",
            lockGuardFactory.createDeferred().write(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).build(),
            () ->
            {
                Flux<ApiCallRc> flux = Flux.empty();
                try
                {
                    Map<String, String> objRefs = new TreeMap<>();
                    objRefs.put(ApiConsts.KEY_NODE, nodeName);

                    AccessContext peerCtx = peerAccCtx.get();
                    Node node = ctrlApiDataLoader.loadNode(nodeName, true);

                    StateFlags<Node.Flags> nodeFlags = node.getFlags();
                    boolean wasNodeEvicted = nodeFlags.isSet(peerCtx, Node.Flags.EVICTED);
                    boolean wasNodeEvacuated = nodeFlags.isSet(peerCtx, Node.Flags.EVACUATE);
                    if (!wasNodeEvacuated && !wasNodeEvicted)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl
                                .simpleEntry(
                                    ApiConsts.MASK_ERROR,
                                    "Node '" + nodeName + "' is neither evicted nor evacuated."
                                )
                                .setSkipErrorReport(true)
                        );
                    }
                    node.getObjProt().requireAccess(peerCtx, AccessType.CONTROL);
                    nodeFlags.disableFlags(peerCtx, Node.Flags.EVACUATE, Node.Flags.EVICTED);

                    Iterator<Resource> rscIt = node.iterateResources(peerCtx);
                    if (deleteResources)
                    {
                        while (rscIt.hasNext())
                        {
                            Resource rsc = rscIt.next();

                            // false if not a DRBD resource. false -> delete this resource
                            boolean isLastNonDeletedDiskful = LayerRscUtils.getLayerStack(rsc, apiCtx)
                                .contains(DeviceLayerKind.DRBD);
                            {
                                ResourceDefinition rscDfn = rsc.getResourceDefinition();
                                Iterator<Resource> rscIt2 = rscDfn.iterateResource(apiCtx);
                                while (rscIt2.hasNext())
                                {
                                    Resource otherRsc = rscIt2.next();
                                    if (!otherRsc.equals(rsc))
                                    {
                                        StateFlags<Flags> rscFlags = otherRsc.getStateFlags();
                                        if (!rscFlags.isSet(apiCtx, Resource.Flags.DISKLESS) &&
                                            !rscFlags.isSet(apiCtx, Resource.Flags.DELETE))
                                        {
                                            isLastNonDeletedDiskful = false;
                                            break;
                                        }
                                    }
                                }
                            }
                            if (!isLastNonDeletedDiskful)
                            {
                                flux = flux.concatWith(
                                    rscDeleteHandler.deleteResource(
                                        nodeName,
                                        rsc.getResourceDefinition().getName().displayValue
                                    )
                                );
                            }
                            else
                            {
                                flux = flux.concatWithValues(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.WARN_STLT_NOT_UPDATED,
                                        "Not deleting resource " + rsc.getResourceDefinition().getName().displayValue +
                                            " since it is the last non-deleting diskful resource of" +
                                            " this resource-definition"
                                    )
                                );
                                errorReporter.logDebug(
                                    "Auto-evict: ignoring resource %s since it is the last non-deleting " +
                                        "diskful resource",
                                    rsc.getResourceDefinition().getName()
                                );
                            }
                        }
                    }
                    else
                    {
                        while (rscIt.hasNext())
                        {
                            Resource rsc = rscIt.next();
                            StateFlags<Flags> flags = rsc.getStateFlags();
                            boolean updateSatellite = false;
                            if (flags.isSet(peerCtx, Resource.Flags.EVICTED))
                            {
                                flags.disableFlags(peerCtx, Resource.Flags.EVICTED);
                                if (flags.isSet(peerCtx, Resource.Flags.INACTIVE_BEFORE_EVICTION))
                                {
                                    flags.enableFlags(peerCtx, Resource.Flags.INACTIVE);
                                    flags.disableFlags(peerCtx, Resource.Flags.INACTIVE_BEFORE_EVICTION);
                                }
                                // although we will perform a FullSync soon, the other satellites also need to be
                                // updated that this resource is back online
                                updateSatellite = true;
                            }
                            if (flags.isSet(peerCtx, Resource.Flags.EVACUATE))
                            {
                                flags.disableFlags(peerCtx, Resource.Flags.EVACUATE);
                                updateSatellite = true;
                            }

                            ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);

                            if (updateSatellite)
                            {
                                flux = flux.concatWith(
                                    ctrlSatelliteUpdateCaller.updateSatellites(
                                            rsc.getResourceDefinition(),
                                            CtrlSatelliteUpdateCaller.notConnectedIgnoreIf(node.getName()),
                                            Flux.empty())
                                        .transform(
                                            responses -> CtrlResponseUtils.combineResponses(
                                                errorReporter,
                                                responses,
                                                rsc.getResourceDefinition().getName(),
                                                "Resource restored on {0}"
                                            )
                                        )
                                );
                            }
                        }
                    }

                    if (deleteSnapshots)
                    {
                        for (Snapshot snap : node.getSnapshots(peerCtx))
                        {
                            flux = flux.concatWith(
                                snapDeleteHandler.deleteSnapshot(
                                    snap.getResourceName(),
                                    snap.getSnapshotName(),
                                    Collections.singletonList(snap.getNodeName().displayValue)
                                )
                            );
                        }
                    }
                    ApiCallRcImpl rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_NODE,
                        "Successfully restored node " + nodeName
                    );
                    AutoHelperContext autoCtx = new AutoHelperContext(
                        rc,
                        new ResponseContext(
                            ApiOperation.makeModifyOperation(),
                            getNodeDescription(nodeName),
                            getNodeDescriptionInline(nodeName),
                            ApiConsts.MASK_SUCCESS | ApiConsts.MASK_NODE,
                            objRefs
                        ),
                        null
                    );
                    Flux<ApiCallRc> autoFlux = ctrlRscAutoHelper.manageAll(autoCtx);

                    ctrlTransactionHelper.commit();

                    eventNodeHandlerBridge.triggerNodeRestored(node.getApiData(peerCtx, null, null));

                    if (wasNodeEvicted)
                    {
                        reconnectorTask.add(
                            node.getPeer(peerCtx).getConnector().reconnect(node.getPeer(peerCtx)),
                            false,
                            false
                        );
                    }
                    Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateFlux = ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        node.getName(),
                        CtrlSatelliteUpdater.findNodesToContact(peerCtx, node)
                    );

                    flux = flux.concatWith(updateFlux.transform(tuple -> Flux.empty()))
                        .concatWith(autoFlux)
                        .concatWithValues(rc);
                }
                catch (AccessDeniedException exc)
                {
                    errorReporter.reportError(exc);
                    ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_ACC_DENIED_NODE | ApiConsts.MASK_NODE,
                        "Access to node " + nodeName + " denied"
                    );
                    flux = Flux.just(rc);
                }
                catch (DatabaseException exc)
                {
                    String rep = errorReporter.reportError(exc);
                    ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_SQL | ApiConsts.MASK_NODE,
                        "Database Error, see error report " + rep
                    );
                    flux = Flux.just(rc);
                }
                return flux;
            }
        );
    }

    public Flux<ApiCallRc> evictNode(String nodeName)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Evict node",
            lockGuardFactory.createDeferred().write(LockObj.NODES_MAP).build(),
            () ->
            {
                Node node = ctrlApiDataLoader.loadNode(nodeName, true);
                try
                {
                    if (node.getPeer(apiCtx).isOnline())
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_IN_USE | ApiConsts.MASK_NODE,
                                "Eviction of an online node is not possible."
                            )
                        );
                    }
                }
                catch (AccessDeniedException exc)
                {
                    throw new ApiAccessDeniedException(exc, "to " + nodeName, ApiConsts.FAIL_ACC_DENIED_NODE);
                }
                return declareEvicted(node);
            },
            MDC.getCopyOfContextMap()
        );
    }

    public Flux<ApiCallRc> declareEvicted(Node node)
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Declare node EVICTED",
                lockGuardFactory.createDeferred().write(LockObj.NODES_MAP).build(),
                () ->
                {
                    node.markEvicted(apiCtx);
                    Flux<ApiCallRc> flux = Flux.empty();
                    for (Resource res : node.streamResources(apiCtx).collect(Collectors.toList()))
                    {
                        if (LayerRscUtils.getLayerStack(res, apiCtx).contains(DeviceLayerKind.DRBD))
                        {
                            Map<String, String> objRefs = new TreeMap<>();
                            objRefs.put(ApiConsts.KEY_RSC_DFN, res.getResourceDefinition().getName().displayValue);
                            objRefs.put(ApiConsts.KEY_NODE, res.getNode().getName().displayValue);

                            ResponseContext context = new ResponseContext(
                                ApiOperation.makeDeleteOperation(),
                                "Auto-evicting resource: " + res.getResourceDefinition().getName(),
                                "auto-evicting resource: " + res.getResourceDefinition().getName(),
                                ApiConsts.MASK_MOD,
                                objRefs
                            );
                            AutoHelperContext autoHelperCtx = new AutoHelperContext(
                                new ApiCallRcImpl(),
                                context,
                                res.getResourceDefinition()
                            );

                            StateFlags<Flags> rscFlags = res.getStateFlags();
                            if (rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE))
                            {
                                rscFlags.enableFlags(apiCtx, Resource.Flags.INACTIVE_BEFORE_EVICTION);
                            }
                            rscFlags.enableFlags(apiCtx, Resource.Flags.EVICTED);
                            autoRePlaceRscHelper.addNeedRePlaceRsc(res);
                            autoRePlaceRscHelper.manage(autoHelperCtx);

                            flux = flux.concatWith(Flux.concat(autoHelperCtx.additionalFluxList))
                                .concatWith(
                                    this.ctrlSatelliteUpdateCaller.updateSatellites(
                                        res.getResourceDefinition(),
                                        CtrlSatelliteUpdateCaller.notConnectedWarn(),
                                        Flux.empty()
                                    )
                                        .transform(
                                        responses -> CtrlResponseUtils.combineResponses(
                                            errorReporter,
                                            responses,
                                            res.getResourceDefinition().getName(),
                                            "Resource updated on {0}"
                                        )
                                    )
                                );
                        }
                        else
                        {
                            errorReporter.logDebug(
                                "Auto-evict: ignoring resource %s since it is a non-DRBD resource",
                                res.getResourceDefinition().getName()
                            );
                        }
                    }
                    ctrlTransactionHelper.commit();
                    eventNodeHandlerBridge.triggerNodeEvicted(node.getApiData(apiCtx, null, null));
                    flux = flux.concatWith(ctrlBackupCrtApiCallHandler.deleteNodeQueueAndReQueueSnapsIfNeeded(node));
                    flux = ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        node.getName(),
                        CtrlSatelliteUpdater.findNodesToContact(apiCtx, node)
                    )
                        .transform(tuple -> Flux.<ApiCallRc>empty())
                        .concatWith(flux);
                    return flux;
                }
            )
        );
    }

    public Flux<ApiCallRc> evacuateNode(
        String nodeNameRef,
        @Nullable List<String> allowedTargetNodeNameStrListRef,
        @Nullable List<String> prohibitedNodeNamesStrListRef
    )
    {
        ResponseContext context = makeNodeContext(
            ApiOperation.makeModifyOperation(),
            nodeNameRef
        );

        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Evacuate node (" + nodeNameRef + "): Fetch free spaces",
                lockGuardFactory.createDeferred().write(LockObj.NODES_MAP).build(),
                () -> scopeRunner.fluxInTransactionalScope(
                    "Evacuate node (" + nodeNameRef + "): Find new storage pools",
                    lockGuardFactory.createDeferred().write(LockObj.NODES_MAP).build(),
                    () -> evacuateNodeInTrasaction(
                        context,
                        nodeNameRef,
                        allowedTargetNodeNameStrListRef,
                        prohibitedNodeNamesStrListRef
                    )
                )
            )
        );
    }

    private Flux<ApiCallRc> evacuateNodeInTrasaction(
        ResponseContext contextRef,
        String nodeNameEvacuateSourceStrRef,
        @Nullable List<String> allowedTargetNodeNameStrListRef,
        @Nullable List<String> prohibitedNodeNamesStrListRef
    )
    {
        Flux<ApiCallRc> flux;
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        Node nodeToEvacuate = ctrlApiDataLoader.loadNode(nodeNameEvacuateSourceStrRef, true);
        NodeName nodeNameEvacuateSource = nodeToEvacuate.getName();
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            nodeToEvacuate.getFlags().enableFlags(peerCtx, Node.Flags.EVACUATE);

            PriorityProps prioProps = new PriorityProps(
                nodeToEvacuate.getProps(apiCtx),
                ctrlPropsHelper.getCtrlPropsForView()
            );
            @Nullable String copyAllSnapsStr = prioProps.getProp(ApiConsts.KEY_COPY_ALL_SNAPS_ON_EVAC);
            boolean copyAllSnaps = copyAllSnapsStr == null || Boolean.parseBoolean(copyAllSnapsStr);

            LinkedHashSet<ResourceDefinition> affectedRscDfnList = new LinkedHashSet<>();
            Iterator<Resource> rscIt = nodeToEvacuate.iterateResources(peerCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                rsc.getStateFlags().enableFlags(peerCtx, Resource.Flags.EVACUATE);
                rsc.getProps(peerAccCtx.get()).map().put(ApiConsts.KEY_RSC_MIGRATE_FROM, nodeNameEvacuateSource.value);
                affectedRscDfnList.add(rsc.getResourceDefinition());
            }

            if (copyAllSnaps)
            {
                /*
                 * If we want to evacuate all snapshots, we first register a "post-ship delete snapshot"-flux
                 * and in the later cases (create target rsc, toggle-disk target rsc, no target rsc at all) we also make
                 * sure to properly start the shipments.
                 */
                for (Snapshot snap : nodeToEvacuate.getSnapshots(peerCtx))
                {
                    affectedRscDfnList.add(snap.getResourceDefinition());
                }
            }

            List<Flux<ApiCallRc>> fluxList = new ArrayList<>();
            for (ResourceDefinition rscDfn : affectedRscDfnList)
            {
                ResourceName rscName = rscDfn.getName();
                if (!rscDfn.usesLayer(peerCtx, DeviceLayerKind.DRBD))
                {
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.WARN_NOT_EVACUATING,
                            "Resource '" + rscName.displayValue +
                                "' cannot be evacuated as it is not a DRBD resource"
                        )
                    );
                }
                else
                {
                    // rscToEvac might be null if we have added the RD from a snapshot
                    @Nullable Resource rscToEvacuate = rscDfn.getResource(peerCtx, nodeNameEvacuateSource);
                    if (rscToEvacuate != null)
                    {
                        StateFlags<Flags> rscToEvacFlags = rscToEvacuate.getStateFlags();
                        boolean justDeleteRscToEvac = rscToEvacFlags.isSet(peerCtx, Resource.Flags.DRBD_DISKLESS);

                        if (!justDeleteRscToEvac)
                        {
                            // no use to keep a non-UpToDate resource that should be evacuated
                            justDeleteRscToEvac = !SatelliteResourceStateDrbdUtils.allVolumesUpToDate(
                                apiCtx,
                                rscToEvacuate
                            );
                        }

                        int expectedReplicaCount = rscDfn.getResourceGroup()
                            .getAutoPlaceConfig()
                            .getReplicaCount(peerCtx);
                        if (!justDeleteRscToEvac)
                        {
                            int upToDatePeerCount = getUpToDatePeerCount(peerCtx, rscDfn);
                            if (upToDatePeerCount >= expectedReplicaCount)
                            {
                                justDeleteRscToEvac = true;
                            }
                        }

                        if (justDeleteRscToEvac)
                        {
                            if (copyAllSnaps)
                            {
                                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerCtx))
                                {
                                    @Nullable Snapshot snap = snapDfn.getSnapshot(peerCtx, nodeNameEvacuateSource);
                                    if (snap != null)
                                    {
                                        StorPool sp = getStorPoolForEvacuation(
                                            rscDfn,
                                            allowedTargetNodeNameStrListRef,
                                            prohibitedNodeNamesStrListRef
                                        );
                                        if (sp == null)
                                        {
                                            sp = getStorPoolForCopySnaps(rscDfn, snapDfn);
                                        }
                                        if (sp == null)
                                        {
                                            apiCallRc.addEntry(
                                                ApiCallRcImpl.simpleEntry(
                                                    ApiConsts.WARN_NOT_EVACUATING,
                                                    "Snapshot '" + snapDfn.getName() + "' of resource " +
                                                        rscName.displayValue +
                                                        "' cannot be evacuated as no available storage pool was found"
                                                )
                                            );
                                        }
                                        else
                                        {
                                            NodeName nodeNameEvacTarget = sp.getNode().getName();
                                            int snapCount = snapDfn.getAllNotDeletingSnapshots(apiCtx).size();
                                            if (snapCount == 1 || snapCount - 1 < expectedReplicaCount)
                                            {
                                                copySnapsHelper.deleteSnapAfterShipmentSent(snap, false);
                                                fluxList.add(
                                                    copySnapsHelper.getCopyFlux(
                                                        snapDfn,
                                                        nodeNameEvacTarget.displayValue,
                                                        contextRef,
                                                        true
                                                    )
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            fluxList.add(
                                rscDeleteHandler.deleteResource(
                                    nodeNameEvacuateSourceStrRef,
                                    rscDfn.getName().displayValue
                                )
                            );
                        }
                        else
                        {
                            StorPool sp = getStorPoolForEvacuation(
                                rscDfn,
                                allowedTargetNodeNameStrListRef,
                                prohibitedNodeNamesStrListRef
                            );
                            if (sp == null)
                            {
                                apiCallRc.addEntry(
                                    ApiCallRcImpl.simpleEntry(
                                        ApiConsts.WARN_NOT_EVACUATING,
                                        "Resource '" + rscName.displayValue +
                                            "' cannot be evacuated as no available storage pool was found"
                                    )
                                );
                            }
                            else
                            {
                                NodeName nodeNameEvacTarget = sp.getNode().getName();

                                Flux<ApiCallRc> createOrToggleDiskFlux = null;
                                @Nullable Resource rscOnTargetNode = sp.getNode().getResource(peerCtx, rscName);
                                if (copyAllSnaps)
                                {
                                    for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerCtx))
                                    {
                                        @Nullable Snapshot snap = snapDfn.getSnapshot(peerCtx, nodeNameEvacuateSource);
                                        if (snap != null)
                                        {
                                            copySnapsHelper.deleteSnapAfterShipmentSent(snap, false);
                                        }
                                    }
                                }
                                if (rscOnTargetNode != null)
                                {
                                    // selected node already has the resource
                                    if (rscOnTargetNode.getStateFlags().isSet(peerCtx, Resource.Flags.DRBD_DISKLESS))
                                    {
                                        createOrToggleDiskFlux = rscToggleDiskApiCallHandler.resourceToggleDisk(
                                            nodeNameEvacTarget.displayValue,
                                            rscName.displayValue,
                                            sp.getName().displayValue,
                                            nodeNameEvacuateSourceStrRef,
                                            null,
                                            false,
                                            Resource.DiskfulBy.AUTO_PLACER,
                                            false,
                                            copyAllSnaps,
                                            Collections.emptyList(),
                                            true,
                                            null // will be fetched
                                        );
                                    }
                                }
                                if (createOrToggleDiskFlux == null)
                                {
                                    ResourceWithPayloadPojo createRscPojo = new ResourceWithPayloadPojo(
                                        new RscPojo(
                                            rscName.displayValue,
                                            nodeNameEvacTarget.displayValue,
                                            0L,
                                            Collections.singletonMap(
                                                ApiConsts.KEY_STOR_POOL_NAME,
                                                sp.getName().displayValue
                                            )
                                        ),
                                        rscDfn.getLayerStack(peerCtx)
                                            .stream()
                                            .map(DeviceLayerKind::name)
                                            .collect(Collectors.toList()),
                                        null,
                                        null,
                                        null
                                    );
                                    createOrToggleDiskFlux = ctrlRscCrtApiCallHandler.createResource(
                                        Collections.singletonList(createRscPojo),
                                        Resource.DiskfulBy.AUTO_PLACER,
                                        copyAllSnaps,
                                        Collections.emptyList(),
                                        true
                                    )
                                        .concatWith(
                                            rscToggleDiskApiCallHandler.waitForMigration(
                                                contextRef,
                                                nodeNameEvacTarget,
                                                rscName,
                                                nodeNameEvacuateSource
                                            )
                                        );
                                }
                                fluxList.add(createOrToggleDiskFlux);
                            }
                        }
                    }
                    else
                    {
                        // we have added the RD because we have a snapshot
                        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerCtx))
                        {
                            @Nullable Snapshot snap = snapDfn.getSnapshot(peerCtx, nodeNameEvacuateSource);
                            if (snap != null)
                            {
                                StorPool sp = getStorPoolForEvacuation(
                                    rscDfn,
                                    allowedTargetNodeNameStrListRef,
                                    prohibitedNodeNamesStrListRef
                                );
                                if (sp == null)
                                {
                                    sp = getStorPoolForCopySnaps(rscDfn, snapDfn);
                                }
                                if (sp == null)
                                {
                                    apiCallRc.addEntry(
                                        ApiCallRcImpl.simpleEntry(
                                            ApiConsts.WARN_NOT_EVACUATING,
                                            "Snapshot '" + snapDfn.getName() + "' of resource " + rscName.displayValue +
                                                "' cannot be evacuated as no available storage pool was found"
                                        )
                                    );
                                }
                                else
                                {
                                    if (copyAllSnaps)
                                    {
                                        copySnapsHelper.deleteSnapAfterShipmentSent(snap, false);
                                    }
                                    NodeName nodeNameEvacTarget = sp.getNode().getName();
                                    fluxList.add(
                                        copySnapsHelper.getCopyFlux(
                                            snapDfn,
                                            nodeNameEvacTarget.displayValue,
                                            contextRef,
                                            true
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }

            ctrlTransactionHelper.commit();
            eventNodeHandlerBridge.triggerNodeEvacuate(nodeToEvacuate.getApiData(peerCtx, null, null));
            flux = Flux.<ApiCallRc>just(apiCallRc)
                .concatWith(
                    // we must make sure to first interrupt all currently ongoing shipments
                    // before we start our evacuation shipments (otherwise those could get interrupted as well)
                    ctrlBackupCrtApiCallHandler.deleteNodeQueueAndReQueueSnapsIfNeeded(nodeToEvacuate)
                )
                .concatWith(Flux.merge(fluxList));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update the node type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return flux;
    }

    private int getUpToDatePeerCount(AccessContext accCtx, ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        int ret = 0;
        Iterator<Resource> rscIt = rscDfnRef.iterateResource(accCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            if (rscFlags.isUnset(
                accCtx,
                Resource.Flags.DELETE,
                Resource.Flags.EVACUATE,
                Resource.Flags.INACTIVE,
                Resource.Flags.DRBD_DISKLESS
            ))
            {
                boolean allDrbdVolumesUpToDate = SatelliteResourceStateDrbdUtils.allVolumesUpToDate(
                    accCtx,
                    rsc,
                    false
                );
                if (allDrbdVolumesUpToDate)
                {
                    ret++;
                }
            }
        }
        return ret;
    }

    private @Nullable StorPool getStorPoolForEvacuation(
        ResourceDefinition rscDfn,
        @Nullable List<String> allowedTargetNodeNameStrListRef,
        @Nullable List<String> prohibitedNodeNamesStrListRef
    )
        throws AccessDeniedException
    {
        AccessContext peerCtx = peerAccCtx.get();

        @Nullable Set<StorPool> storPoolSet = null;
        List<String> allowedNodeNames = getAllowedNodeNamesStr(
            allowedTargetNodeNameStrListRef,
            prohibitedNodeNamesStrListRef
        );

        // first try to toggle disk if possible
        Set<Resource> disklessRscSet = ResourceUtils.filterResourcesDrbdDiskless(rscDfn, peerCtx);
        long rscSize = CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerCtx);
        if (!disklessRscSet.isEmpty())
        {
            errorReporter.logTrace("searching for diskless replacement to toggle-disk");
            List<String> disklessNodeNameStrList = disklessRscSet.stream()
                .map(rsc -> rsc.getNode().getName().displayValue)
                .collect(Collectors.toList());

            disklessNodeNameStrList.retainAll(allowedNodeNames);

            storPoolSet = autoplacer.autoPlace(
                AutoSelectFilterPojo.merge(
                    new AutoSelectFilterBuilder()
                        .setNodeNameList(disklessNodeNameStrList)
                        .setSkipAlreadyPlacedOnNodeNamesCheck(disklessNodeNameStrList)
                        .build(),
                    rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                ),
                rscDfn,
                rscSize
            );
        }

        AutoSelectFilterPojo autoSelectNodeNameFilter = new AutoSelectFilterBuilder()
            .setNodeNameList(allowedNodeNames)
            .build();

        if (storPoolSet == null || storPoolSet.isEmpty())
        {
            errorReporter.logTrace("searching for diskful replacements to fulfill replCt");
            // no storage pool found for toggle disk. find any other storage pool
            storPoolSet = autoplacer.autoPlace(
                AutoSelectFilterPojo.merge(
                    autoSelectNodeNameFilter,
                    rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                ),
                rscDfn,
                rscSize
            );
        }

        if (storPoolSet == null || storPoolSet.isEmpty())
        {
            errorReporter.logTrace("searching for single diskful replacement ignoring replCt");
            // configured place-count could not be met, but maybe we can still place the resource at least on one new
            // node to keep the current replica count
            storPoolSet = autoplacer.autoPlace(
                AutoSelectFilterPojo.merge(
                    autoSelectNodeNameFilter,
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(rscDfn.getResourceCount()) // EVACUATE flag already set, therefore that rsc will
                                                                  // not count -> effectively "+1" while overriding the
                                                                  // configuration set in rscGrp
                        .build(),
                        rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                ),
                rscDfn,
                rscSize
            );
        }

        @Nullable StorPool ret = null;
        if (storPoolSet != null && !storPoolSet.isEmpty())
        {
            ret = storPoolSet.iterator().next();
        }
        return ret;
    }

    private @Nullable StorPool getStorPoolForCopySnaps(ResourceDefinition rscDfn, SnapshotDefinition snapDfn)
        throws AccessDeniedException
    {
        // if placing only a snapshot, limit nodes to those that don't already have the snap,
        // and disable the rscAlreadyPlacedOn rule
        errorReporter.logTrace("searching for rsc to add snapshots to");
        long rscSize = CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, apiCtx);
        List<String> nodesWithoutSnap = new ArrayList<>();
        for (Resource rsc : rscDfn.getDiskfulResources(apiCtx))
        {
            NodeName nodeName = rsc.getNode().getName();
            if (snapDfn.getSnapshot(apiCtx, nodeName) == null)
            {
                nodesWithoutSnap.add(nodeName.displayValue);
            }
        }
        Set<StorPool> storPoolSet = autoplacer.autoPlace(
            AutoSelectFilterPojo.merge(
                new AutoSelectFilterBuilder().setNodeNameList(nodesWithoutSnap)
                    .setSkipAlreadyPlacedOnAllNodeCheck(true)
                    .build(),
                rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
            ),
            rscDfn,
            rscSize
        );
        return storPoolSet != null && !storPoolSet.isEmpty() ? storPoolSet.iterator().next() : null;
    }

    private List<String> getAllowedNodeNamesStr(
        @Nullable List<String> allowedTargetNodeNameStrListRef,
        @Nullable List<String> prohibitedNodeNamesStrListRef
    )
    {
        List<String> ret = new ArrayList<>();
        try
        {
            if (!CollectionUtils.isEmpty(allowedTargetNodeNameStrListRef))
            {
                if (!CollectionUtils.isEmpty(prohibitedNodeNamesStrListRef))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_CONF,
                            "Must not use --target and --do-not-target in the same request"
                        )
                    );
                }
                for (NodeName nodeName : nodeRepository.getMapForView(apiCtx).keySet())
                {
                    if (CollectionUtils.contains(nodeName, allowedTargetNodeNameStrListRef))
                    {
                        ret.add(nodeName.displayValue);
                    }
                }
            }
            else
            {
                for (NodeName nodeName : nodeRepository.getMapForView(apiCtx).keySet())
                {
                    if (!CollectionUtils.contains(nodeName, prohibitedNodeNamesStrListRef))
                    {
                        ret.add(nodeName.displayValue);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }
}
