package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeConnectionFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class CtrlNodeConnectionHelper
{
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeConnectionFactory nodeConnectionFactory;

    @Inject
    public CtrlNodeConnectionHelper(
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeConnectionFactory nodeConnectionFactoryRef
    )
    {
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeConnectionFactory = nodeConnectionFactoryRef;
    }

    public @Nullable NodeConnection loadNodeConn(
        String nodeName1,
        String nodeName2,
        boolean failIfNull,
        boolean createIfNotExists
    )
    {
        // when the connection would be created on demand, the nodes themselves still have to
        // exist - otherwise createNodeConn would run into a NullPointerException
        boolean nodesMustExist = failIfNull || createIfNotExists;
        @Nullable Node node1 = nodesMustExist ?
            ctrlApiDataLoader.loadNode(nodeName1) :
            ctrlApiDataLoader.loadNodeOrNull(nodeName1);
        @Nullable Node node2 = nodesMustExist ?
            ctrlApiDataLoader.loadNode(nodeName2) :
            ctrlApiDataLoader.loadNodeOrNull(nodeName2);

        NodeConnection nodeConn = null;
        if (node1 != null && node2 != null)
        {
            nodeConn = NodeConnection.get(
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
        return nodeConn;
    }

    public NodeConnection createNodeConn(Node node1, Node node2)
    {
        NodeConnection nodeConnection;
        try
        {
            nodeConnection = nodeConnectionFactory.create(
                node1,
                node2
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
            nodeConnRef.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
