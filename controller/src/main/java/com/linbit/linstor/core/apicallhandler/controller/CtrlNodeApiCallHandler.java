package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.PortAlreadyInUseException;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.SatelliteConfigApi;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeControllerFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.Props;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

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
        @Named(NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL) DynamicNumberPool sfTargetPortPoolRef,
        SpecialSatelliteProcessManager ofTargetProcMgrRef,
        ReconnectorTask reconnectorTaskRef,
        Scheduler schedulerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlStltSerializer stltComSerializerRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        CtrlRscAutoRePlaceRscHelper autoRePlaceRscHelperRef,
        ErrorReporter errorReporterRef,
        FreeCapacityFetcher freeCapacityFetcherRef
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
        specStltPortPool = sfTargetPortPoolRef;
        specStltProcMgr = ofTargetProcMgrRef;
        reconnectorTask = reconnectorTaskRef;
        scheduler = schedulerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltComSerializer = stltComSerializerRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        autoRePlaceRscHelper = autoRePlaceRscHelperRef;
        errorReporter = errorReporterRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
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

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.NODE,
                propsMap,
                ctrlPropsHelper.getProps(node),
                ApiConsts.FAIL_ACC_DENIED_NODE);

            for (NetInterfaceApi netIfApi : netIfs)
            {
                TcpPortNumber port = null;
                EncryptionType encrType = null;
                if (netIfApi.isUsableAsSatelliteConnection())
                {
                    port = LinstorParsingUtils.asTcpPortNumber(netIfApi.getSatelliteConnectionPort());
                    encrType = LinstorParsingUtils.asEncryptionType(netIfApi.getSatelliteConnectionEncryptionType());
                }

                NetInterface netIf = createNetInterface(
                    node,
                    LinstorParsingUtils.asNetInterfaceName(netIfApi.getName()),
                    LinstorParsingUtils.asLsIpAddress(netIfApi.getAddress()),
                    port,
                    encrType
                );

                if (netIfApi.isUsableAsSatelliteConnection() &&
                    getActiveStltConn(node) == null
                )
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
                specStltProcMgr.startLocalSatelliteProcess(nodeNameStr, specStltPort);

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

    private NetInterface getActiveStltConn(Node node)
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
        UUID nodeUuid,
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
        UUID nodeUuid,
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
                setNodeType(node, nodeTypeStr);
                notifyStlts = true;
            }

            Props props = ctrlPropsHelper.getProps(node);
            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.NODE,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_NODE,
                Arrays.asList(ApiConsts.NAMESPC_EXOS)
            ) || notifyStlts;
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs, LinStorObject.NODE, props, deletePropKeys, deleteNamespaces) || notifyStlts;

            flux = flux.concatWith(checkProperties(apiCallRcs, node, overrideProps, deletePropKeys, deleteNamespaces));

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                node.getUuid(), getNodeDescriptionInline(node)));

            if (notifyStlts) {
                flux = ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        nodeName,
                        findNodesToContact(apiCtx, node))
                    .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2());
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs).concatWith(flux);
    }

    public ApiCallRc reconnectNode(
        List<String> nodes
    )
    {
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
                Peer crtNodePeer = node.getPeer(apiCtx); // check for access

                if (!node.getFlags().isSet(apiCtx, Node.Flags.EVICTED))
                {
                    reconnectorTask.add(crtNodePeer, false);
                    // the close connection has to run in its own thread
                    // otherwise we will get re-entering scope problems (Error report)
                    scheduler.schedule(() ->
                        {
                            {
                                try
                                {
                                    node.getPeer(apiCtx).closeConnection(true);
                                }
                                catch (Exception | ImplementationError ignored)
                                {
                                }
                            }
                        }
                    );
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
        return responses;
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
                            final Props props = node.getProps(peerAccCtx.get());
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
        TcpPortNumber port,
        EncryptionType type
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

    private void setNodeType(Node node, String nodeTypeStr)
    {
        Node.Type nodeType = LinstorParsingUtils.asNodeType(nodeTypeStr);
        try
        {
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
            if ((Node.Type.OPENFLEX_TARGET.equals(nodeType) || Node.Type.REMOTE_SPDK.equals(nodeType)) &&
                node.streamNetInterfaces(apiCtx).count() != 1)
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
        ExtToolsInfo drbd9 = node.getPeer(apiCtx).getExtToolsManager().getExtToolInfo(ExtTools.DRBD9);
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

    public Flux<ApiCallRc> setGlobalConfig(SatelliteConfigApi config) throws AccessDeniedException, IOException
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
        throws AccessDeniedException, IOException
    {
        return scopeRunner.fluxInTransactionlessScope(
            "set satellite config",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP),
            () -> setStltConfig(nodeName, config)
        );
    }

    private Flux<ApiCallRc> setStltConfig(String nodeName, SatelliteConfigApi config)
        throws IOException, AccessDeniedException
    {
        Peer peer = ctrlApiDataLoader.loadNode(nodeName, true).getPeer(peerAccCtx.get());
        if (!peer.isConnected())
        {
            return Flux.empty();
        }
        StltConfig stltConf = peer.getStltConfig();
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
        return peer
            .apiCall(InternalApiConsts.API_MOD_STLT_CONFIG, msg)
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
            .map(responseMsg ->
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
                catch (IOException e)
                {
                    rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_NODE,
                        "Failure while updating satellite config"
                    );
                }
                return rc;
            })
            .transform(response -> responseConverter.reportingExceptions(context, response));
    }

    public Flux<ApiCallRc> restoreNode(String nodeName)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Restore EVICTED node",
            lockGuardFactory.createDeferred().write(LockObj.NODES_MAP).build(),
            () ->
            {
                Flux<ApiCallRc> flux = Flux.empty();
                try
                {
                    Node node = ctrlApiDataLoader.loadNode(nodeName, true);
                    node.unMarkEvicted(apiCtx);
                    ctrlTransactionHelper.commit();
                    reconnectorTask
                        .add(node.getPeer(apiCtx).getConnector().reconnect(node.getPeer(apiCtx)), false, false);
                    Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateFlux = ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        node.getName(),
                        CtrlSatelliteUpdater.findNodesToContact(apiCtx, node)
                    );
                    ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_NODE,
                        "Successfully restored node " + nodeName
                    );
                    flux = updateFlux.transform(tuple -> Flux.empty());
                    return flux.concatWithValues(rc);
                }
                catch (AccessDeniedException exc)
                {
                    errorReporter.reportError(exc);
                    ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_ACC_DENIED_NODE | ApiConsts.MASK_NODE,
                        "Access to node " + nodeName + " denied"
                    );
                    return flux.concatWithValues(rc);
                }
                catch (DatabaseException exc)
                {
                    String rep = errorReporter.reportError(exc);
                    ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_SQL | ApiConsts.MASK_NODE,
                        "Database Error, see error report " + rep
                    );
                    return flux.concatWithValues(rc);
                }
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
                    if (node.getPeer(apiCtx).isConnected())
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
            }
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
                    ctrlTransactionHelper.commit();
                    Flux<ApiCallRc> flux = ctrlSatelliteUpdateCaller.updateSatellites(
                        node.getUuid(),
                        node.getName(),
                        CtrlSatelliteUpdater.findNodesToContact(apiCtx, node)
                    )
                        .transform(tuple -> Flux.empty());
                    for (Resource res : node.streamResources(apiCtx).collect(Collectors.toList()))
                    {
                        if (LayerRscUtils.getLayerStack(res, apiCtx).contains(DeviceLayerKind.DRBD))
                        {
                            Map<String, String> objRefs = new TreeMap<>();
                            objRefs.put(ApiConsts.KEY_RSC_DFN, res.getDefinition().getName().displayValue);
                            objRefs.put(ApiConsts.KEY_NODE, res.getNode().getName().displayValue);

                            ResponseContext context = new ResponseContext(
                                ApiOperation.makeDeleteOperation(),
                                "Auto-evicting resource: " + res.getDefinition().getName(),
                                "auto-evicting resource: " + res.getDefinition().getName(),
                                ApiConsts.MASK_DEL,
                                objRefs
                            );
                            AutoHelperContext autoHelperCtx = new AutoHelperContext(
                                new ApiCallRcImpl(),
                                context,
                                res.getDefinition()
                            );

                            boolean isLastNonDeletedDiskful = true;
                            {
                                ResourceDefinition rscDfn = res.getResourceDefinition();
                                Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
                                while (rscIt.hasNext())
                                {
                                    Resource rsc = rscIt.next();
                                    if (!rsc.equals(res))
                                    {
                                        StateFlags<Flags> rscFlags = rsc.getStateFlags();
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
                                res.markDeleted(apiCtx);
                                Iterator<Volume> vlmIt = res.iterateVolumes();
                                while (vlmIt.hasNext())
                                {
                                    Volume vlm = vlmIt.next();
                                    vlm.markDeleted(apiCtx);
                                }
                                autoRePlaceRscHelper.addNeedRePlaceRsc(res);
                                autoRePlaceRscHelper.manage(autoHelperCtx);

                                flux = flux.concatWith(Flux.concat(autoHelperCtx.additionalFluxList));
                            }
                            else
                            {
                                errorReporter.logDebug(
                                    "Auto-evict: ignoring resource %s since it is the last non-deleting diskful resource",
                                    res.getDefinition().getName()
                                );
                            }


                            flux = flux.concatWith(Flux.concat(autoHelperCtx.additionalFluxList));
                        }
                        else
                        {
                            errorReporter.logDebug(
                                "Auto-evict: ignoring resource %s since it is a non-DRBD resource",
                                res.getDefinition().getName()
                            );
                        }
                    }
                    ctrlTransactionHelper.commit();
                    return flux;
                }
            )
        );
    }
}
