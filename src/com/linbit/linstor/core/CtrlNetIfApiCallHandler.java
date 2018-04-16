package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

class CtrlNetIfApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeName;
    private String currentNetIfName;
    private final SatelliteConnector satelliteConnector;
    private final NetInterfaceDataFactory netInterfaceDataFactory;

    @Inject
    CtrlNetIfApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        @ApiContext AccessContext apiCtxRef,
        SatelliteConnector satelliteConnectorRef,
        CtrlObjectFactories objectFactories,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.NET_IF,
            serializerRef,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        satelliteConnector = satelliteConnectorRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
    }

    public ApiCallRc createNetIf(
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
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStr,
                netIfNameStr
            );
        )
        {
            NodeData node = loadNode(nodeNameStr, true);
            NetInterfaceName netIfName = asNetInterfaceName(netIfNameStr);

            if (node.getSatelliteConnection(apiCtx) != null && stltPort != null)
            {
                throw asExc(
                    new LinStorException("This node has already a satellite connection defined"),
                    "Only one satellite connection allowed",
                    ApiConsts.FAIL_EXISTS_STLT_CONN
                );
            }

            NetInterfaceData netIf = createNetIf(node, netIfName, address, stltPort, stltEncrType);

            if (stltPort != null && stltEncrType != null)
            {
                satelliteConnector.startConnecting(node, apiCtx);
            }

            commit();
            reportSuccess(netIf.getUuid());
            updateSatellites(node);
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
                apiCallRc
            );
        }
        return apiCallRc;
    }

    public ApiCallRc modifyNetIf(
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
                ApiCallType.MODIFY,
                apiCallRc,
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
                needsReconnect |= setStltConn(netIf, stltPort, stltEncrType);

                NetInterface currStltConn = getSatelliteConnection(node);
                if (currStltConn == null)
                {
                    setSatelliteConnection(node, netIf);
                }
            }

            commit();

            reportSuccess(netIf.getUuid());

            if (needsReconnect)
            {
                node.getPeer(apiCtx).closeConnection();
                satelliteConnector.startConnecting(node, apiCtx);
            }
            updateSatellites(node);
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
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private NetInterface getSatelliteConnection(Node node)
    {
        NetInterface netIf = null;
        try
        {
            netIf = node.getSatelliteConnection(peerAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "access the current satellite connection",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netIf;
    }

    private void setSatelliteConnection(Node node, NetInterface netIf)
    {
        try
        {
            node.setSatelliteConnection(peerAccCtx, netIf);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "set the current satellite connection",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "updating satellite connection"
            );
        }
    }


    public ApiCallRc deleteNetIf(
        String nodeNameStr,
        String netIfNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr,
                netIfNameStr
            );
        )
        {

            NetInterface netIf = loadNetIf(nodeNameStr, netIfNameStr, false);
            if (netIf == null)
            {
                addAnswer(
                    "Deletion of " + getObjectDescriptionInline() + " had no effect.",
                    getObjectDescriptionInlineFirstLetterCaps() + " does not exist.",
                    null,
                    null,
                    ApiConsts.WARN_NOT_FOUND
                );
                throw new ApiCallHandlerFailedException();
            }
            else
            {
                Node node = netIf.getNode();
                boolean closeConnection = false;
                closeConnection = netIf.equals(
                    node.getSatelliteConnection(apiCtx)
                );

                UUID uuid = netIf.getUuid();
                deleteNetIf(netIf);

                commit();
                reportSuccess(uuid);

                if (closeConnection)
                {
                    // updateSatellites(node);
                    // FIXME: updating a satellite is risky here.
                    // when the sending takes too long, the connection will be already closed (next statement)
                    // for now, just close the connection. once a new connection is established, the
                    // satellite gets a full sync anyways
                    node.getPeer(apiCtx).closeConnection();
                }
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
                apiCallRc
            );
        }

        return apiCallRc;
    }

    private NetInterfaceData createNetIf(
        NodeData node,
        NetInterfaceName netIfName,
        String address,
        Integer stltConnPort,
        String stltConnEncrType
    )
    {
        NetInterfaceData netIf;
        try
        {
            TcpPortNumber port = null;
            EncryptionType type = null;
            if (stltConnPort != null && stltConnEncrType != null)
            {
                port = asTcpPortNumber(stltConnPort);
                type = asEncryptionType(stltConnEncrType);
            }

            netIf = netInterfaceDataFactory.getInstance(
                peerAccCtx,
                node,
                netIfName,
                asLsIpAddress(address),
                port,
                type,
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

    private EncryptionType asEncryptionType(String stltConnEncrType)
    {
        EncryptionType type = null;
        try
        {
            type = EncryptionType.valueOfIgnoreCase(stltConnEncrType);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            throw asExc(illegalArgExc, "Invalid encryption type", ApiConsts.FAIL_INVLD_NET_TYPE);
        }
        return type;
    }

    private NetInterface loadNetIf(String nodeNameStr, String netIfNameStr)
    {
        return loadNetIf(nodeNameStr, netIfNameStr, true);
    }

    private NetInterface loadNetIf(String nodeNameStr, String netIfNameStr, boolean failIfNull)
    {
        Node node = loadNode(nodeNameStr, true);
        NetInterface netIf;
        try
        {
            netIf = node.getNetInterface(
                peerAccCtx,
                asNetInterfaceName(netIfNameStr)
            );

            if (failIfNull && netIf == null)
            {
                throw asExc(
                    null,
                    "Node '" + nodeNameStr + "' has no network interface named '" + netIfNameStr + "'.",
                    ApiConsts.FAIL_NOT_FOUND_NET_IF
                );
           }
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
            netIf.setAddress(peerAccCtx, asLsIpAddress(addressStr));
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

    private boolean setStltConn(NetInterface netIf, Integer stltPort, String stltEncrType)
    {
        boolean changed = false;
        try
        {
            changed = netIf.setStltConn(peerAccCtx, asTcpPortNumber(stltPort), asEncryptionType(stltEncrType));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "modify the satellite connection port and / or encryption type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "updating the net interface's satellite connection port and / or encryption type"
            );
        }
        return changed;
    }

    private boolean addressChanged(NetInterface netIf, String addressStr)
    {
        boolean ret;
        try
        {
            ret = !netIf.getAddress(peerAccCtx).getAddress().equals(addressStr);
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

    private void deleteNetIf(NetInterface netIf)
    {
        try
        {
            netIf.delete(peerAccCtx);
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
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr,
        String netIfNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeNameStr),
            getVariables(nodeNameStr, netIfNameStr)
        );
        currentNodeName = nodeNameStr;
        currentNetIfName = netIfNameStr;

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
        return getObjectDescription(currentNodeName, currentNetIfName);
    }

    public static String getObjectDescription(String nodeName, String netIfName)
    {
        return "Node: '" + nodeName + "', NetIfName: " + netIfName + "'";
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName, currentNetIfName);
    }

    public static String getObjectDescriptionInline(String nodeName, String netIfName)
    {
        return "netInterface '" + netIfName + "' on node '" + nodeName + "'";
    }
}
