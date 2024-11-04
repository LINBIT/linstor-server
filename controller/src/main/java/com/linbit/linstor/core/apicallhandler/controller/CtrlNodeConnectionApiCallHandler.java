package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.NodeConnectionApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.PairNonNull;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlNodeConnectionApiCallHandler
{
    private static final String PATTERN_PATH_PATH_NAME = "pathname";
    private static final String PATTERN_PATH_NODE_NAME = "nodename";
    private static final Pattern PATTERN_PATH = Pattern.compile(
        "^" + ApiConsts.NAMESPC_CONNECTION_PATHS + "/(?<" + PATTERN_PATH_PATH_NAME + ">[^/]+)/(?<" +
            PATTERN_PATH_NODE_NAME + ">[^/]+)$"
    );

    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlNodeConnectionHelper nodeConnectionHelper;
    private final NodeRepository nodeRepo;

    @Inject
    public CtrlNodeConnectionApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlNodeConnectionHelper nodeConnectionHelperRef,
        NodeRepository nodeRepoRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeConnectionHelper = nodeConnectionHelperRef;
        nodeRepo = nodeRepoRef;
    }

    public Collection<NodeConnectionApi> listNodeConnections(@Nullable String nodeARef, @Nullable String nodeBRef)
    {
        TreeSet<NodeConnectionApi> ret;

        if (nodeARef == null && nodeBRef == null)
        {
            AccessContext peerCtx = peerAccCtx.get();
            ret = new TreeSet<>();
            try
            {
                for (Node node : nodeRepo.getMapForView(peerCtx).values())
                {
                    ret.addAll(getNodeConnPojos(node, false));
                }
            }
            catch (AccessDeniedException exc)
            {
                // noop, just do not include in list
            }
        }
        else if (nodeARef == null)
        {
            ret = getNodeConnPojos(ctrlApiDataLoader.loadNode(nodeBRef, true), false);
        }
        else if (nodeBRef == null)
        {
            ret = getNodeConnPojos(ctrlApiDataLoader.loadNode(nodeARef, true), false);
        }
        else
        {
            Node nodeA = ctrlApiDataLoader.loadNode(nodeARef, true);
            Node nodeB = ctrlApiDataLoader.loadNode(nodeBRef, true);
            AccessContext peerCtx = peerAccCtx.get();
            ret = new TreeSet<>();
            try
            {
                NodeConnection nodeConn = nodeA.getNodeConnection(
                    peerCtx,
                    nodeB
                );
                if (nodeConn == null)
                {
                    ret.add(
                        new NodeConnPojo(
                            null,
                            nodeARef,
                            nodeB.getApiData(peerCtx, null, null),
                            Collections.emptyMap()
                        )
                    );
                }
                else
                {
                    ret.add(
                        nodeConn.getApiData(nodeA, peerCtx, null, null)
                    );
                }
            }
            catch (AccessDeniedException exc)
            {
                // noop, don't add to list
            }
        }
        return ret;
    }

    private TreeSet<NodeConnectionApi> getNodeConnPojos(Node node, boolean includeIfEmpty)
    {
        TreeSet<NodeConnectionApi> ret = new TreeSet<>();
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            for (NodeConnection nodeConn : node.getNodeConnections(peerCtx))
            {
                if (includeIfEmpty || !nodeConn.getProps(peerCtx).isEmpty())
                {
                    ret.add(
                        nodeConn.getApiData(
                            nodeConn.getSourceNode(peerCtx),
                            peerCtx,
                            null,
                            null
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            // noop, just do not include in list
        }
        return ret;
    }
    /**
     * Modifies an existing nodeConnection
     *
     * @param nodeConnUuid
     *     optional, if given checks against persisted uuid
     * @param nodeName1
     *     required
     * @param nodeName2
     *     required
     * @param overridePropsRef
     *     optional, can be empty
     * @param deletePropKeysRef
     *     optional, can be empty
     */
    public Flux<ApiCallRc> modifyNodeConn(
        @Nullable UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        final Map<String, String> overrideProps = overridePropsRef == null ? Collections.emptyMap() : overridePropsRef;
        final Set<String> deletePropKeys = deletePropKeysRef == null ? Collections.emptySet() : deletePropKeysRef;
        ResponseContext context = makeNodeConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2
        );
        return scopeRunner.fluxInTransactionalScope(
            "Modify node-connection",
            lockGuardFactory.buildDeferred(WRITE, NODES_MAP),
            () -> modifyNodeConnectionInTransaction(
                nodeConnUuid,
                nodeName1,
                nodeName2,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            )
        )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> modifyNodeConnectionInTransaction(
        @Nullable UUID nodeConnUuidRef,
        String nodeName1,
        String nodeName2,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> flux;
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2
        );

        try
        {
            boolean createIfNotExists = !overrideProps.isEmpty();
            NodeConnection nodeConn = nodeConnectionHelper.loadNodeConn(
                nodeName1,
                nodeName2,
                false,
                createIfNotExists
            );

            if (nodeConn == null)
            {
                responses.addEntry(
                    CtrlNodeConnectionHelper.getNodeConnectionDescription(nodeName1, nodeName2) + " does not exist",
                    ApiConsts.WARN_NOT_FOUND
                );
                flux = Flux.just(responses);
            }
            else
            {
                UUID nodeConnUuid = nodeConn.getUuid();
                if (nodeConnUuidRef != null && !nodeConnUuidRef.equals(nodeConnUuid))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UUID_NODE_CONN,
                            "UUID-check failed"
                        )
                    );
                }

                Props props = getProps(nodeConn);

                for (Entry<String, String> entry : overrideProps.entrySet())
                {
                    String key = entry.getKey();
                    Matcher matcher = PATTERN_PATH.matcher(key);
                    if (matcher.find())
                    {
                        String nodeName = matcher.group(PATTERN_PATH_NODE_NAME);
                        if (!nodeName.equals(nodeName1) && !nodeName.equals(nodeName2))
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl
                                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Connection path node unknown.")
                                    .setCause("The node name '" + nodeName + "' is unknown.")
                                    .build()
                            );
                        }
                        // now check that the interface name are correct/existing
                        Node node = nodeConn.getNode(peerAccCtx.get(), new NodeName(nodeName));
                        NetInterface netInterface = node.getNetInterface(
                            peerAccCtx.get(),
                            new NetInterfaceName(entry.getValue())
                        );
                        if (netInterface == null)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl
                                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "NetInterface for node unknown.")
                                    .setCause(
                                        String.format(
                                            "The Netinterface '%s' is not known for node '%s'",
                                            entry.getValue(),
                                            nodeName
                                        )
                                    )
                                    .build()
                            );
                        }
                    }
                    else if (key.startsWith(ApiConsts.NAMESPC_CONNECTION_PATHS + "/"))
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl
                                .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Connection path property invalid.")
                                .setCause("The key '" + entry + "' is invalid.")
                                .build()
                        );
                    }
                }

                ctrlPropsHelper.fillProperties(
                    responses,
                    LinStorObject.NODE_CONN,
                    overrideProps,
                    props,
                    ApiConsts.FAIL_ACC_DENIED_NODE_CONN,
                    new ArrayList<>(Arrays.asList(ApiConsts.NAMESPC_CONNECTION_PATHS + "/"))
                );
                ctrlPropsHelper.remove(
                    responses,
                    LinStorObject.NODE_CONN,
                    props,
                    deletePropKeys,
                    deletePropNamespaces,
                    new ArrayList<>(Arrays.asList(ApiConsts.NAMESPC_CONNECTION_PATHS + "/"))
                );

                ctrlTransactionHelper.commit();

                responseConverter.addWithOp(
                    responses,
                    context,
                    ApiSuccessUtils.defaultModifiedEntry(
                        nodeConnUuid,
                        CtrlNodeConnectionHelper.getNodeConnectionDescriptionInline(nodeName1, nodeName2)
                    )
                );
                responseConverter.addWithDetail(responses, context, updateSatellites(nodeConn));

                PairNonNull<Node, Node> nodes = getNodes(nodeConn);

                flux = Flux.<ApiCallRc>just(responses)
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            nodes.objA.getUuid(),
                            nodes.objA.getName(),
                            Arrays.asList(nodes.objB)
                        ).flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
                    )
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            nodes.objB.getUuid(),
                            nodes.objB.getName(),
                            Arrays.asList(nodes.objA)
                        ).flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
                );

                if (nodeConn.getProps(apiCtx).isEmpty())
                {
                    flux = flux.concatWith(cleanupNodeConn(nodeConn));
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
            flux = Flux.just(responses);
        }

        return flux;
    }

    private PairNonNull<Node, Node> getNodes(NodeConnection nodeConnRef)
    {
        AccessContext peerCtx = peerAccCtx.get();
        try
        {
            return new PairNonNull<>(nodeConnRef.getSourceNode(peerCtx), nodeConnRef.getTargetNode(peerCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "accessing " + CtrlNodeConnectionHelper.getNodeConnectionDescriptionInline(nodeConnRef),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
    }

    private Props getProps(NodeConnection nodeConn)
    {
        Props props;
        try
        {
            props = nodeConn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessing properties of node connection '" + nodeConn + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        return props;
    }

    private ApiCallRc updateSatellites(NodeConnection nodeConn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            responses.addEntries(ctrlSatelliteUpdater.updateSatellites(nodeConn.getSourceNode(apiCtx)));
            responses.addEntries(ctrlSatelliteUpdater.updateSatellites(nodeConn.getTargetNode(apiCtx)));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return responses;
    }

    private Flux<ApiCallRc> cleanupNodeConn(NodeConnection nodeConnectionRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Cleaning up node connection",
            lockGuardFactory.buildDeferred(WRITE, NODES_MAP),
            () -> cleanupNodeConnInTransaction(nodeConnectionRef)
        );
    }

    private Flux<ApiCallRc> cleanupNodeConnInTransaction(NodeConnection nodeConnRef)
    {
        if (nodeConnRef != null && !nodeConnRef.isDeleted() && getProps(nodeConnRef).isEmpty())
        {
            errorReporter.logDebug(
                "Deleting empty %s",
                CtrlNodeConnectionHelper.getNodeConnectionDescriptionInline(nodeConnRef)
            );
            nodeConnectionHelper.deleteNodeConnection(nodeConnRef);

            ctrlTransactionHelper.commit();
        }
        return Flux.empty();
    }

    private static ResponseContext makeNodeConnectionContext(
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);

        return new ResponseContext(
            operation,
            CtrlNodeConnectionHelper.getNodeConnectionDescription(nodeName1Str, nodeName2Str),
            CtrlNodeConnectionHelper.getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str),
            ApiConsts.MASK_NODE_CONN,
            objRefs
        );
    }
}
