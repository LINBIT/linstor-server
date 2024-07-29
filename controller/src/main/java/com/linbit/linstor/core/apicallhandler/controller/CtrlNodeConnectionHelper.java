package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeConnectionFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class CtrlNodeConnectionHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeConnectionFactory nodeConnectionFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlNodeConnectionHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeConnectionFactory nodeConnectionFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeConnectionFactory = nodeConnectionFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public @Nullable NodeConnection loadNodeConn(
        String nodeName1,
        String nodeName2,
        boolean failIfNull,
        boolean createIfNotExists
    )
    {
        Node node1 = ctrlApiDataLoader.loadNode(nodeName1, failIfNull);
        Node node2 = ctrlApiDataLoader.loadNode(nodeName2, failIfNull);

        NodeConnection nodeConn = null;
        try
        {
            if (node1 != null && node2 != null)
            {
                nodeConn = NodeConnection.get(
                    peerAccCtx.get(),
                    node1,
                    node2
                );
            }

            if (nodeConn == null)
            {
                if (failIfNull)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_NODE_CONN,
                            "Failed to load " + getNodeConnectionDescriptionInline(
                                nodeName1,
                                nodeName2
                            ) + " as it does not exist"
                        )
                    );
                }
                if (createIfNotExists)
                {
                    nodeConn = createNodeConn(node1, node2);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getNodeConnectionDescriptionInline(node1, node2),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        return nodeConn;
    }

    public NodeConnection createNodeConn(Node node1, Node node2)
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
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_NODE_CONN,
                    getNodeConnectionDescription(node1, node2) + " already exists."
                ),
                dataAlreadyExistsExc
            );
        }
        return nodeConnection;
    }

    public void deleteNodeConnection(NodeConnection nodeConnRef)
    {
        try
        {
            nodeConnRef.delete(peerAccCtx.get());
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "deleting " + getNodeConnectionDescriptionInline(nodeConnRef),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
    }

    public static String getNodeConnectionDescriptionInline(NodeConnection nodeConnRef)
    {
        return getNodeConnectionDescriptionInline(
            nodeConnRef.getSourceNodeName().displayValue,
            nodeConnRef.getTargetNodeName().displayValue
        );
    }

    public static String getNodeConnectionDescriptionInline(Node node1, Node node2)
    {
        return getNodeConnectionDescriptionInline(
            node1.getName().displayValue,
            node2.getName().displayValue
        );
    }

    public static String getNodeConnectionDescriptionInline(String nodeName1Str, String nodeName2Str)
    {
        return "node connection between nodes '" + nodeName1Str + "' and '" + nodeName2Str + "'";
    }


    public static String getNodeConnectionDescription(Node node1, Node node2)
    {
        return getNodeConnectionDescription(
            node1.getName().displayValue,
            node2.getName().displayValue
        );
    }

    public static String getNodeConnectionDescription(String nodeName1Str, String nodeName2Str)
    {
        return "Node connection between nodes '" + nodeName1Str + "' and '" + nodeName2Str + "'";
    }
}
