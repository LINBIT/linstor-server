package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.SatelliteConnection;
import com.linbit.linstor.SatelliteConnection.EncryptionType;
import com.linbit.linstor.SatelliteConnectionData;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
class CtrlNetIfApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentNetIfName = new ThreadLocal<>();
    private final Props ctrlConf;
    private final SatelliteConnector satelliteConnector;
    private final NetComContainer netComContainer;

    @Inject
    CtrlNetIfApiCallHandler(
        ErrorReporter errorReporterRef,
        DbConnectionPool dbConnectionPoolRef,
        CtrlStltSerializer serializerRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        SatelliteConnector satelliteConnectorRef,
        NetComContainer netComContainerRef
    )
    {
        super(errorReporterRef, dbConnectionPoolRef, apiCtxRef, ApiConsts.MASK_NET_IF, serializerRef);
        super.setNullOnAutoClose(
            currentNodeName,
            currentNetIfName
        );
        ctrlConf = ctrlConfRef;
        satelliteConnector = satelliteConnectorRef;
        netComContainer = netComContainerRef;
    }

    public ApiCallRc createNetIf(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String netIfNameStr,
        String address,
        Integer stltPort,
        String stltEncrType
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

            if (node.getSatelliteConnection(apiCtx) != null && stltPort != null)
            {
                throw asExc(
                    new LinStorException("This node has already a satellite connection defined"),
                    "Only one satellite connection allowed",
                    ApiConsts.FAIL_EXISTS_STLT_CONN
                );
            }
            if (stltPort != null && stltEncrType != null)
            {
                createStltConn(node, netIf, stltPort, stltEncrType);
                startConnecting(node);
            }

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
        String addressStr,
        Integer stltPort,
        String stltEncrType
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
            boolean needsReconnect = addressChanged(netIf, addressStr);

            setAddress(netIf, addressStr);

            Node node = netIf.getNode();
            if (stltPort != null && stltEncrType != null)
            {
                SatelliteConnection stltConn = node.getSatelliteConnection(apiCtx);
                if (stltPort != null && stltEncrType != null)
                {
                    if (stltConn == null)
                    {
                        createStltConn(node, netIf, stltPort, stltEncrType);
                    }
                    else
                    {
                        needsReconnect |= setStltPort(stltConn, stltPort);
                        needsReconnect |= setStltEncrType(stltConn, stltEncrType);
                    }
                }
            }

            commit();

            reportSuccess(netIf.getUuid());

            if (needsReconnect)
            {
                node.getPeer(apiCtx).closeConnection();
                startConnecting(node);
            }
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

            Node node = netIf.getNode();
            boolean closeConnection = netIf.equals(
                node.getSatelliteConnection(apiCtx).getNetInterface()
            );

            UUID uuid = netIf.getUuid();
            deleteNetIf(netIf);

            commit();

            reportSuccess(uuid);
            if (closeConnection)
            {
                node.getPeer(apiCtx).closeConnection();
            }
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

    private NetInterfaceData createNetIf(
        NodeData node,
        NetInterfaceName netIfName,
        String address
    )
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

    private void createStltConn(
        Node node,
        NetInterface netIf,
        Integer stltPort,
        String stltEncrType
    )
    {
        try
        {
            SatelliteConnectionData.getInstance(
                currentAccCtx.get(),
                node,
                netIf,
                asTcpPortNumber(stltPort),
                SatelliteConnection.EncryptionType.valueOf(stltEncrType.toUpperCase()),
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
                ApiConsts.FAIL_ACC_DENIED_STLT_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw asExc(
                exc,
                getObjectDescriptionInlineFirstLetterCaps() + " already exists",
                ApiConsts.FAIL_EXISTS_STLT_CONN
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "creating " + getObjectDescriptionInline()
            );
        }
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

    private boolean addressChanged(NetInterface netIf, String addressStr)
    {
        boolean ret;
        try
        {
            ret = !netIf.getAddress(currentAccCtx.get()).getAddress().equals(addressStr);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "accessing " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return ret;
    }

    private boolean setStltPort(SatelliteConnection stltConn, int stltPort)
    {
        boolean changed = stltConn.getPort().value != stltPort;
        try
        {
            stltConn.setPort(currentAccCtx.get(), asTcpPortNumber(stltPort));
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "updating satellite port for " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "updating satellite port for " + getObjectDescriptionInline()
            );
        }
        return changed;
    }

    private boolean setStltEncrType(SatelliteConnection stltConn, String stltEncrType)
    {
        boolean changed = stltConn.getEncryptionType().name().equals(stltEncrType);
        try
        {
            stltConn.setEncryptionType(
                currentAccCtx.get(),
                EncryptionType.valueOf(
                    stltEncrType.toUpperCase()
                )
            );
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "updating satellite encryption type for " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "updating satellite port for " + getObjectDescriptionInline()
            );
        }
        return changed;
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

    private void startConnecting(Node node)
    {
        CtrlNodeApiCallHandler.startConnecting(
            node,
            apiCtx,
            satelliteConnector,
            ctrlConf,
            netComContainer
        );
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
