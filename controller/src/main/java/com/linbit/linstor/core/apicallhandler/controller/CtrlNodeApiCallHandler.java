package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater.findNodesToContact;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

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
    private final ReconnectorTask reconnectorTask;
    private final Scheduler scheduler;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;

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
        ReconnectorTask reconnectorTaskRef,
        Scheduler schedulerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef
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
        reconnectorTask = reconnectorTaskRef;
        scheduler = schedulerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
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
                LinStorObject.NODE, propsMap, ctrlPropsHelper.getProps(node), ApiConsts.FAIL_ACC_DENIED_NODE);

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
                        (String) null
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
        NetInterface netIf = null;
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
            }

            Props props = ctrlPropsHelper.getProps(node);
            ctrlPropsHelper.fillProperties(LinStorObject.NODE, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_NODE);
            ctrlPropsHelper.remove(LinStorObject.NODE, props, deletePropKeys, deleteNamespaces);

            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                apiCtx,
                node,
                overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                ApiConsts.MASK_NODE
            );

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                node.getUuid(), getNodeDescriptionInline(node)));

            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                node.getUuid(),
                nodeName,
                findNodesToContact(apiCtx, node)
            )
            .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2());
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

        try
        {
            for (String nodeStr : nodes)
            {
                Node node = ctrlApiDataLoader.loadNode(new NodeName(nodeStr), true);
                Peer crtNodePeer = node.getPeer(apiCtx); // check for access

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
            }

            responses.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_MOD | ApiConsts.MASK_NODE | ApiConsts.MASK_SUCCESS,
                "Nodes [" + String.join(",", nodes) + "] will be reconnected."
            ));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }
        return responses;
    }

    ArrayList<NodeApi> listNodes(List<String> nodeNames)
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
                            nodes.add(node.getApiData(peerAccCtx.get(), null, null));
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
}
