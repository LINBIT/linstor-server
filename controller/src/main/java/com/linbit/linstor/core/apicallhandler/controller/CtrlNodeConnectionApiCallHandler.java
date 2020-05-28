package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeConnectionFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlNodeConnectionApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeConnectionFactory nodeConnectionFactory;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlNodeConnectionApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeConnectionFactory nodeConnectionFactoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeConnectionFactory = nodeConnectionFactoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc createNodeConnection(
        String nodeName1Str,
        String nodeName2Str,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str
        );

        try
        {
            Node node1 = ctrlApiDataLoader.loadNode(nodeName1Str, true);
            Node node2 = ctrlApiDataLoader.loadNode(nodeName2Str, true);

            NodeConnection nodeConn = createNodeConn(node1, node2);

            boolean notifyStlts = ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.NODE_CONN,
                nodeConnPropsMap,
                getProps(nodeConn),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                nodeConn.getUuid(), getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str)));

            if (notifyStlts) {
                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(node1));
                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(node2));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyNodeConnection(
        UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2
        );

        try
        {
            NodeConnection nodeConn = loadNodeConn(nodeName1, nodeName2, true);

            if (nodeConnUuid != null && !nodeConnUuid.equals(nodeConn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_NODE_CONN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(nodeConn);

            ctrlPropsHelper.fillProperties(
                responses, LinStorObject.NODE_CONN, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_NODE_CONN);
            ctrlPropsHelper.remove(responses, LinStorObject.NODE_CONN, props, deletePropKeys, deletePropNamespaces);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                nodeConn.getUuid(), getNodeConnectionDescriptionInline(nodeName1, nodeName2)));
            responseConverter.addWithDetail(responses, context, updateSatellites(nodeConn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteNodeConnection(
        String nodeName1Str,
        String nodeName2Str
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str
        );

        try
        {
            NodeConnection nodeConn = loadNodeConn(nodeName1Str, nodeName2Str, false);
            if (nodeConn == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    "Could not delete " + getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str) +
                        "as it does not exist"
                ));
            }
            else
            {
                UUID nodeConnUuid = nodeConn.getUuid();
                nodeConn.delete(peerAccCtx.get()); // accDeniedExc4

                ctrlTransactionHelper.commit();

                responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                    nodeConnUuid, getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str)));

                responseConverter.addWithDetail(responses, context, updateSatellites(nodeConn));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private NodeConnection createNodeConn(Node node1, Node node2)
    {
        NodeConnection nodeConnection;
        try
        {
            nodeConnection = nodeConnectionFactory.create(
                peerAccCtx.get(),
                node1,
                node2
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getNodeConnectionDescriptionInline(node1, node2),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_NODE_CONN,
                getNodeConnectionDescription(node1, node2) + " already exists."
            ), dataAlreadyExistsExc);
        }
        return nodeConnection;
    }

    private NodeConnection loadNodeConn(String nodeName1, String nodeName2, boolean failIfNull)
    {
        Node node1 = ctrlApiDataLoader.loadNode(nodeName1, true);
        Node node2 = ctrlApiDataLoader.loadNode(nodeName2, true);

        NodeConnection nodeConn;
        try
        {
            nodeConn = NodeConnection.get(
                peerAccCtx.get(),
                node1,
                node2
            );
            if (nodeConn == null && failIfNull)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_NODE_CONN,
                    "Failed to load " + getNodeConnectionDescriptionInline(nodeName1, nodeName2) +
                        " as it does not exist"
                ));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load node connection between nodes '" + nodeName1 + "' and '" + nodeName2 + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        return nodeConn;
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

    public static String getNodeConnectionDescription(Node node1, Node node2)
    {
        return getNodeConnectionDescription(node1.getName().displayValue, node2.getName().displayValue);
    }

    public static String getNodeConnectionDescription(String nodeName1Str, String nodeName2Str)
    {
        return "Node connection between " + nodeName1Str + " and " + nodeName2Str;
    }

    public static String getNodeConnectionDescriptionInline(Node node1, Node node2)
    {
        return getNodeConnectionDescriptionInline(node1.getName().displayValue, node2.getName().displayValue);
    }

    public static String getNodeConnectionDescriptionInline(String nodeName1Str, String nodeName2Str)
    {
        return "node connection between nodes '" + nodeName1Str + "' and '" + nodeName2Str + "'";
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
            getNodeConnectionDescription(nodeName1Str, nodeName2Str),
            getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str),
            ApiConsts.MASK_NODE_CONN,
            objRefs
        );
    }
}
