package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class CtrlNetIfApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentNetIfName = new ThreadLocal<>();

    @Inject
    CtrlNetIfApiCallHandler(
        ErrorReporter errorReporterRef,
        DbConnectionPool dbConnectionPoolRef,
        CtrlStltSerializer serializerRef,
        @ApiContext AccessContext apiCtxRef
    )
    {
        super(errorReporterRef, dbConnectionPoolRef, apiCtxRef, ApiConsts.MASK_NET_IF, serializerRef);
        super.setNullOnAutoClose(
            currentNodeName,
            currentNetIfName
        );
    }

    public ApiCallRc createNetIf(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String netIfNameStr,
        String address
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null,
                nodeNameStr,
                netIfNameStr
            );
        )
        {
            NodeData node = loadNode(nodeNameStr, true);
            NetInterfaceName netIfName = asNetInterfaceName(netIfNameStr);

            NetInterfaceData netIf = createNetIf(node, netIfName, address);

            commit();
            reportSuccess(netIf.getUuid());
        }
        catch (ApiCallHandlerFailedException ignored)
        {
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeNameStr, netIfNameStr),
                getObjRefs(nodeNameStr),
                getVariables(nodeNameStr, netIfNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }
        return apiCallRc;
    }

    public ApiCallRc modifyNetIf(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String netIfNameStr,
        String addressStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null,
                nodeNameStr,
                netIfNameStr
            );
        )
        {
            NetInterface netIf = loadNetIf(nodeNameStr, netIfNameStr);
            setAddress(netIf, addressStr);

            commit();

            reportSuccess(netIf.getUuid());
        }
        catch (ApiCallHandlerFailedException ignored)
        {
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, netIfNameStr),
                getObjRefs(nodeNameStr),
                getVariables(nodeNameStr, netIfNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteNetIf(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String netIfNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null,
                nodeNameStr,
                netIfNameStr
            );
        )
        {
            NetInterface netIf = loadNetIf(nodeNameStr, netIfNameStr);

            UUID uuid = netIf.getUuid();
            deleteNetIf(netIf);

            commit();

            reportSuccess(uuid);
        }
        catch (ApiCallHandlerFailedException ignored)
        {
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeNameStr, netIfNameStr),
                getObjRefs(nodeNameStr),
                getVariables(nodeNameStr, netIfNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private NetInterfaceData createNetIf(NodeData node, NetInterfaceName netIfName, String address)
    {
        NetInterfaceData netIf;
        try
        {
            netIf = NetInterfaceData.getInstance(
                currentAccCtx.get(),
                node,
                netIfName,
                asLsIpAddress(address),
                currentTransMgr.get(),
                true,
                true
            );
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw asExc(
                exc,
                getObjectDescriptionInlineFirstLetterCaps() + " already exists.",
                ApiConsts.FAIL_EXISTS_NET_IF
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "creating " + getObjectDescriptionInline()
            );
        }
        return netIf;
    }


    private NetInterface loadNetIf(String nodeNameStr, String netIfNameStr)
    {
        Node node = loadNode(nodeNameStr, true);
        NetInterface netIf;
        try
        {
            netIf = node.getNetInterface(
                currentAccCtx.get(),
                asNetInterfaceName(netIfNameStr)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "loading " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netIf;
    }

    private void setAddress(NetInterface netIf, String addressStr)
    {
        try
        {
            netIf.setAddress(currentAccCtx.get(), asLsIpAddress(addressStr));
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "setting address of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "updating address of " + getObjectDescriptionInline()
            );
        }
    }

    private void deleteNetIf(NetInterface netIf)
    {
        try
        {
            netIf.delete(currentAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "deleting " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer client,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String netIfNameStr
    )
    {
        super.setContext(
            accCtx,
            client,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(nodeNameStr),
            getVariables(nodeNameStr, netIfNameStr)
        );
        currentNodeName.set(nodeNameStr);
        currentNetIfName.set(netIfNameStr);

        return this;
    }

    private Map<String, String> getObjRefs(String nodeNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        return map;
    }

    private Map<String, String> getVariables(String nodeNameStr, String netIfNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        map.put(ApiConsts.KEY_NET_IF_NAME, netIfNameStr);
        return map;
    }

    @Override
    protected String getObjectDescription()
    {
        return getObjectDescription(currentNodeName.get(), currentNetIfName.get());
    }

    public static String getObjectDescription(String nodeName, String netIfName)
    {
        return "Node: '" + nodeName + "', NetIfName: " + netIfName + "'";
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName.get(), currentNetIfName.get());
    }

    public static String getObjectDescriptionInline(String nodeName, String netIfName)
    {
        return "netInterface '" + netIfName + "' on node '" + nodeName + "'";
    }
}
