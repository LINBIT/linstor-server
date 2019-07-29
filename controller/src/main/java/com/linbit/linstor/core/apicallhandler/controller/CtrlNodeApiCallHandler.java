package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataControllerFactory;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.PortAlreadyInUseException;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.SwordfishTargetProcessManager;
import com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.ReconnectorTask;

import static com.linbit.linstor.api.ApiConsts.DEFAULT_NETIF;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.scheduler.Scheduler;

@Singleton
public class CtrlNodeApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeDataControllerFactory nodeDataFactory;
    private final NetInterfaceDataFactory netInterfaceDataFactory;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final SatelliteConnector satelliteConnector;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final StorPoolHelper storPoolHelper;
    private final DynamicNumberPool sfTargetPortPool;
    private final SwordfishTargetProcessManager sfTargetProcessMgr;
    private final ReconnectorTask reconnectorTask;
    private final Scheduler scheduler;

    @Inject
    public CtrlNodeApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeDataControllerFactory nodeDataFactoryRef,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        SatelliteConnector satelliteConnectorRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        StorPoolHelper storPoolHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(NumberPoolModule.SF_TARGET_PORT_POOL) DynamicNumberPool sfTargetPortPoolRef,
        SwordfishTargetProcessManager sfTargetProcessMgrRef,
        ReconnectorTask reconnectorTaskRef,
        Scheduler schedulerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeDataFactory = nodeDataFactoryRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        satelliteConnector = satelliteConnectorRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        storPoolHelper = storPoolHelperRef;
        peerAccCtx = peerAccCtxRef;
        sfTargetPortPool = sfTargetPortPoolRef;
        sfTargetProcessMgr = sfTargetProcessMgrRef;
        reconnectorTask = reconnectorTaskRef;
        scheduler = schedulerRef;
    }

    NodeData createNodeImpl(
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
        NodeData node = null;
        if (netIfs.isEmpty())
        {
            // TODO for auxiliary nodes maybe no netif required?
            reportMissingNetInterfaces(nodeNameStr); // throws exception
        }
        else
        {
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);

            NodeType type = LinstorParsingUtils.asNodeType(nodeTypeStr);

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

                NetInterfaceData netIf = createNetInterface(
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
                    ApiConsts.WARN_NO_STLT_CONN_DEFINED,
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

    public ApiCallRc createSwordfishTargetNode(String nodeNameStr, Map<String, String> propsMap)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr
        );

        try
        {
            boolean retry = true;
            while (retry)
            {
                retry = false;
                NodeData node = null;
                try
                {
                    int sfTargetPort = sfTargetPortPool.autoAllocate();

                    List<NetInterfaceApi> netIfs = new ArrayList<>();
                    netIfs.add(
                        new NetInterfacePojo(
                            UUID.randomUUID(),
                            DEFAULT_NETIF,
                            "127.0.0.1",
                            sfTargetPort,
                            ApiConsts.VAL_NETCOM_TYPE_PLAIN
                        )
                    );
                    node = createNodeImpl(
                        nodeNameStr,
                        NodeType.SWORDFISH_TARGET.name(),
                        netIfs,
                        propsMap,
                        responses,
                        context,
                        true,
                        false
                    );
                    sfTargetProcessMgr.startLocalSatelliteProcess(node);

                    ctrlTransactionHelper.commit();
                }
                catch (PortAlreadyInUseException exc)
                {
                    /*
                     * By rolling back the transaction, we undo the node-creation.
                     * The process was not started either.
                     * The only thing that remains from our previous try is the port-allocation
                     * of sfTargetPortPool, which should remember that the just tried port is
                     * unavailable.
                     *
                     * The only thing we have to do here is to retry the node-creation, with a new
                     * port number
                     */
                    ctrlTransactionHelper.rollback();
                    if (node != null)
                    {
                        reconnectorTask.removePeer(node.getPeer(apiCtx));
                    }
                    retry = true;
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void setActiveStltConn(Node node, NetInterfaceData netIf)
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

    public ApiCallRc modifyNode(
        UUID nodeUuid,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr
        );

        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
            NodeData node = ctrlApiDataLoader.loadNode(nodeName, true);
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
            ctrlPropsHelper.remove(props, deletePropKeys, deleteNamespaces);

            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                    apiCtx,
                    node,
                    overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                    ApiConsts.MASK_NODE
            );

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(
                responses, context, ctrlSatelliteUpdater.updateSatellites(node));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                node.getUuid(), getNodeDescriptionInline(node)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
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
                NodeData node = ctrlApiDataLoader.loadNode(new NodeName(nodeStr), true);
                node.getPeer(apiCtx); // check for access

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

    ArrayList<Node.NodeApi> listNodes()
    {
        ArrayList<Node.NodeApi> nodes = new ArrayList<>();
        try
        {
            for (Node node : nodeRepository.getMapForView(peerAccCtx.get()).values())
            {
                try
                {
                    nodes.add(node.getApiData(peerAccCtx.get(), null, null));
                    // fullSyncId and updateId null, as they are not going to be serialized by
                    // .nodeList anyways
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add nodes we have not access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return nodes;
    }

    private NodeData createNode(NodeName nodeName, NodeType type)
    {
        NodeData node;
        try
        {
            node = nodeDataFactory.create(
                peerAccCtx.get(),
                nodeName,
                type,
                new NodeFlag[0]
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

    private NetInterfaceData createNetInterface(
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        TcpPortNumber port,
        EncryptionType type
    )
    {
        NetInterfaceData netIf;
        try
        {
            netIf = netInterfaceDataFactory.create(
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

    private void setNodeType(NodeData node, String nodeTypeStr)
    {
        NodeType nodeType = LinstorParsingUtils.asNodeType(nodeTypeStr);
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
            if (NodeType.SWORDFISH_TARGET.equals(nodeType) && node.streamNetInterfaces(apiCtx).count() != 1)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INVLD_NODE_TYPE,
                        "Failed to change node type"
                    )
                    .setCause("A node with type 'swordfish_target' is only allowed to have 1 network interface")
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

    static ResponseContext makeNodeContext(
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
