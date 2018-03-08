package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeConnectionDataFactory;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

class CtrlNodeConnectionApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName1 = new ThreadLocal<>();
    private final ThreadLocal<String> currentNodeName2 = new ThreadLocal<>();
    private final NodeConnectionDataFactory nodeConnectionDataFactory;

    @Inject
    CtrlNodeConnectionApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        NodeConnectionDataFactory nodeConnectionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            ApiConsts.MASK_NODE_CONN,
            interComSerializer,
            objectFactories,
            transMgrProviderRef
        );
        super.setNullOnAutoClose(
            currentNodeName1,
            currentNodeName2
        );
        nodeConnectionDataFactory = nodeConnectionDataFactoryRef;
    }

    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                nodeName1Str,
                nodeName2Str
            );
        )
        {
            NodeData node1 = loadNode(nodeName1Str, true);
            NodeData node2 = loadNode(nodeName2Str, true);

            NodeConnectionData nodeConn = createNodeConn(node1, node2);

            getProps(nodeConn).map().putAll(nodeConnPropsMap);

            commit();

            reportSuccess(nodeConn.getUuid());

            updateSatellites(node1);
            updateSatellites(node2);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeName1Str, nodeName2Str),
                getObjRefs(nodeName1Str, nodeName2Str),
                getVariables(nodeName1Str, nodeName2Str),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc modifyNodeConnection(
        AccessContext accCtx,
        Peer client,
        UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                nodeName1,
                nodeName2
            );
        )
        {
            NodeConnectionData nodeConn = loadNodeConn(nodeName1, nodeName2, true);

            if (nodeConnUuid != null && !nodeConnUuid.equals(nodeConn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_NODE_CONN
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(nodeConn);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            reportSuccess(nodeConn.getUuid());
            updateSatellites(nodeConn);
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeName1, nodeName2),
                getObjRefs(nodeName1, nodeName2),
                getVariables(nodeName1, nodeName2),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                nodeName1Str,
                nodeName2Str
            );
        )
        {
            NodeConnectionData nodeConn = loadNodeConn(nodeName1Str, nodeName2Str, false);
            if (nodeConn == null)
            {
                throw asExc(
                    null,
                    "Could not delete " + getObjectDescriptionInline(nodeName1Str, nodeName2Str) +
                    "as it does not exist",
                    ApiConsts.WARN_NOT_FOUND
                );
            }
            else
            {
                UUID nodeConnUuid = nodeConn.getUuid();
                nodeConn.delete(accCtx); // accDeniedExc4

                commit();

                reportSuccess(nodeConnUuid);

                updateSatellites(nodeConn);
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeName1Str, nodeName2Str),
                getObjRefs(nodeName1Str, nodeName2Str),
                getVariables(nodeName1Str, nodeName2Str),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeName1,
        String nodeName2
    )
    {
        super.setContext(
            accCtx,
            peer,
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeName1, nodeName2),
            getVariables(nodeName1, nodeName2)
        );

        currentNodeName1.set(nodeName1);
        currentNodeName2.set(nodeName2);

        return this;
    }

    private Map<String, String> getObjRefs(String nodeName1Str, String nodeName2Str)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        return objRefs;
    }

    private Map<String, String> getVariables(String nodeName1Str, String nodeName2Str)
    {
        Map<String, String> vars = new TreeMap<>();
        vars.put(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
        vars.put(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
        return vars;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node connection between " + currentNodeName1.get() + " and " + currentNodeName2.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName1.get(), currentNodeName2.get());
    }

    private String getObjectDescriptionInline(String nodeName1Str, String nodeName2Str)
    {
        return "node connection between nodes '" + nodeName1Str + "' and '" + nodeName2Str + "'";
    }

    private NodeConnectionData createNodeConn(NodeData node1, NodeData node2)
    {
        NodeConnectionData nodeConnection;
        try
        {
            nodeConnection = nodeConnectionDataFactory.getInstance(
                currentAccCtx.get(),
                node1,
                node2,
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                getObjectDescription() + " already exists.",
                ApiConsts.FAIL_EXISTS_NODE_CONN
            );
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
                currentAccCtx.get(),
                node1,
                node2,
                false,
                false
            );
            if (nodeConn == null && failIfNull)
            {
                throw asExc(
                    null,
                    "Failed to load " + getObjectDescriptionInline(nodeName1, nodeName2) +
                    " as it does not exist",
                    ApiConsts.FAIL_NOT_FOUND_NODE_CONN
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading node connection between nodes '" + nodeName1 + "' and '" + nodeName2 + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asImplError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading node connection between nodes '" + nodeName1 + "' and '" + nodeName2 + "'."
            );
        }
        return nodeConn;
    }

    private Props getProps(NodeConnectionData nodeConn)
    {
        Props props;
        try
        {
            props = nodeConn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing properties of node connection '" + currentNodeName1.get() + "' <-> '" +
                    currentNodeName2.get() + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE_CONN
            );
        }
        return props;
    }

    private void updateSatellites(NodeConnectionData nodeConn)
    {
        try
        {
            updateSatellites(nodeConn.getSourceNode(apiCtx));
            updateSatellites(nodeConn.getTargetNode(apiCtx));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

}
