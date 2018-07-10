package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeConnectionDataFactory;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlNodeConnectionApiCallHandler extends AbsApiCallHandler
{
    private final NodeConnectionDataFactory nodeConnectionDataFactory;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlNodeConnectionApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        NodeConnectionDataFactory nodeConnectionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        nodeConnectionDataFactory = nodeConnectionDataFactoryRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc createNodeConnection(
        String nodeName1Str,
        String nodeName2Str,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            peer.get(),
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str
        );

        try
        {
            NodeData node1 = loadNode(nodeName1Str, true);
            NodeData node2 = loadNode(nodeName2Str, true);

            NodeConnectionData nodeConn = createNodeConn(node1, node2);

            fillProperties(LinStorObject.NODE_CONN, nodeConnPropsMap, getProps(nodeConn), ApiConsts.FAIL_ACC_DENIED_NODE_CONN);

            commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                nodeConn.getUuid(), getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str)));

            responseConverter.addWithDetail(responses, context, updateSatellites(node1));
            responseConverter.addWithDetail(responses, context, updateSatellites(node2));
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
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeConnectionContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2
        );

        try
        {
            NodeConnectionData nodeConn = loadNodeConn(nodeName1, nodeName2, true);

            if (nodeConnUuid != null && !nodeConnUuid.equals(nodeConn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_NODE_CONN,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(nodeConn);
            Map<String, String> propsMap = props.map();

            fillProperties(LinStorObject.NODE_CONN, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_NODE_CONN);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

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
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str
        );

        try
        {
            NodeConnectionData nodeConn = loadNodeConn(nodeName1Str, nodeName2Str, false);
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

                commit();

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

    private NodeConnectionData createNodeConn(NodeData node1, NodeData node2)
    {
        NodeConnectionData nodeConnection;
        try
        {
            nodeConnection = nodeConnectionDataFactory.getInstance(
                peerAccCtx.get(),
                node1,
                node2,
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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

    private NodeConnectionData loadNodeConn(String nodeName1, String nodeName2, boolean failIfNull)
    {
        NodeData node1 = loadNode(nodeName1, true);
        NodeData node2 = loadNode(nodeName2, true);

        NodeConnectionData nodeConn;
        try
        {
            nodeConn = nodeConnectionDataFactory.getInstance(
                peerAccCtx.get(),
                node1,
                node2,
                false,
                false
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
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return nodeConn;
    }

    private Props getProps(NodeConnectionData nodeConn)
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

    private ApiCallRc updateSatellites(NodeConnectionData nodeConn)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        try
        {
            responses.addEntries(updateSatellites(nodeConn.getSourceNode(apiCtx)));
            responses.addEntries(updateSatellites(nodeConn.getTargetNode(apiCtx)));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return responses;
    }

    public static String getNodeConnectionDescription(NodeData node1, NodeData node2)
    {
        return getNodeConnectionDescription(node1.getName().displayValue, node2.getName().displayValue);
    }

    public static String getNodeConnectionDescription(String nodeName1Str, String nodeName2Str)
    {
        return "Node connection between " + nodeName1Str + " and " + nodeName2Str;
    }

    public static String getNodeConnectionDescriptionInline(NodeData node1, NodeData node2)
    {
        return getNodeConnectionDescriptionInline(node1.getName().displayValue, node2.getName().displayValue);
    }

    public static String getNodeConnectionDescriptionInline(String nodeName1Str, String nodeName2Str)
    {
        return "node connection between nodes '" + nodeName1Str + "' and '" + nodeName2Str + "'";
    }

    private static ResponseContext makeNodeConnectionContext(
        Peer peer,
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);

        return new ResponseContext(
            peer,
            operation,
            getNodeConnectionDescription(nodeName1Str, nodeName2Str),
            getNodeConnectionDescriptionInline(nodeName1Str, nodeName2Str),
            ApiConsts.MASK_NODE_CONN,
            objRefs
        );
    }
}
