package com.linbit.linstor.core.apicallhandler.controller;

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
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.SatelliteConnector;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
class CtrlNetIfApiCallHandler extends AbsApiCallHandler
{
    private final SatelliteConnector satelliteConnector;
    private final NetInterfaceDataFactory netInterfaceDataFactory;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlNetIfApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        @ApiContext AccessContext apiCtxRef,
        SatelliteConnector satelliteConnectorRef,
        CtrlObjectFactories objectFactories,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
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
            serializerRef,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        satelliteConnector = satelliteConnectorRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc createNetIf(
        String nodeNameStr,
        String netIfNameStr,
        String address,
        Integer stltPort,
        String stltEncrType
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            peer.get(),
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            netIfNameStr
        );

        try
        {
            NodeData node = loadNode(nodeNameStr, true);
            NetInterfaceName netIfName = asNetInterfaceName(netIfNameStr);

            if (node.getSatelliteConnection(apiCtx) != null && stltPort != null)
            {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_STLT_CONN,
                        "Only one satellite connection allowed"
                    ), new LinStorException("This node has already a satellite connection defined"));
                }

            NetInterfaceData netIf = createNetIf(node, netIfName, address, stltPort, stltEncrType);

            if (stltPort != null && stltEncrType != null)
            {
                node.setSatelliteConnection(apiCtx, netIf);
                satelliteConnector.startConnecting(node, apiCtx);
            }

            commit();
            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(netIf.getUuid(), getNetIfDescriptionInline(netIf)));
            responseConverter.addWithDetail(responses, context, updateSatellites(node));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc modifyNetIf(
        String nodeNameStr,
        String netIfNameStr,
        String addressStr,
        Integer stltPort,
        String stltEncrType
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            netIfNameStr
        );

        try
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

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                netIf.getUuid(), getNetIfDescriptionInline(netIf)));

            if (needsReconnect)
            {
                node.getPeer(apiCtx).closeConnection();
                satelliteConnector.startConnecting(node, apiCtx);
            }
            responseConverter.addWithDetail(responses, context, updateSatellites(node));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private NetInterface getSatelliteConnection(Node node)
    {
        NetInterface netIf = null;
        try
        {
            netIf = node.getSatelliteConnection(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
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
            node.setSatelliteConnection(peerAccCtx.get(), netIf);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "set the current satellite connection",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
        }
    }


    public ApiCallRc deleteNetIf(
        String nodeNameStr,
        String netIfNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            netIfNameStr
        );

        try
        {
            NetInterface netIf = loadNetIf(nodeNameStr, netIfNameStr, false);
            if (netIf == null)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of " + getNetIfDescriptionInline(nodeNameStr, netIfNameStr) + " had no effect."
                    )
                    .setCause(firstLetterCaps(getNetIfDescriptionInline(nodeNameStr, netIfNameStr)) +
                        " does not exist.")
                    .build()
                );
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
                responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                    uuid, getNetIfDescriptionInline(nodeNameStr, netIfNameStr)));

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
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private NetInterfaceData createNetIf(
        NodeData node,
        NetInterfaceName netIfName,
        String address,
        Integer stltConnPort,
        String stltConnEncrType
    )
    {
        String nodeNameStr = node.getName().displayValue;
        String netIfNameStr = netIfName.displayValue;

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
                peerAccCtx.get(),
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
            throw new ApiAccessDeniedException(
                exc,
                "register " + getNetIfDescriptionInline(nodeNameStr, netIfNameStr),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_NET_IF,
                firstLetterCaps(getNetIfDescriptionInline(nodeNameStr, netIfNameStr)) + " already exists."
            ), exc);
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
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
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_TYPE,
                "Invalid encryption type"
            ), illegalArgExc);
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
                peerAccCtx.get(),
                asNetInterfaceName(netIfNameStr)
            );

            if (failIfNull && netIf == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_NET_IF,
                    "Node '" + nodeNameStr + "' has no network interface named '" + netIfNameStr + "'."
                ));
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "load " + getNetIfDescriptionInline(nodeNameStr, netIfNameStr),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netIf;
    }

    private void setAddress(NetInterface netIf, String addressStr)
    {
        try
        {
            netIf.setAddress(peerAccCtx.get(), asLsIpAddress(addressStr));
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "set address of " + getNetIfDescriptionInline(netIf),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
        }
    }

    private boolean setStltConn(NetInterface netIf, Integer stltPort, String stltEncrType)
    {
        boolean changed = false;
        try
        {
            changed = netIf.setStltConn(peerAccCtx.get(), asTcpPortNumber(stltPort), asEncryptionType(stltEncrType));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "modify the satellite connection port and / or encryption type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return changed;
    }

    private boolean addressChanged(NetInterface netIf, String addressStr)
    {
        boolean ret;
        try
        {
            ret = !netIf.getAddress(peerAccCtx.get()).getAddress().equals(addressStr);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access " + getNetIfDescriptionInline(netIf),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return ret;
    }

    private void deleteNetIf(NetInterface netIf)
    {
        try
        {
            netIf.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + getNetIfDescriptionInline(netIf),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
        }
    }

    public static String getNetIfDescription(String nodeName, String netIfName)
    {
        return "Node: '" + nodeName + "', NetIfName: " + netIfName + "'";
    }

    public static String getNetIfDescriptionInline(NetInterface netIf)
    {
        return getNetIfDescriptionInline(netIf.getNode().getName().displayValue, netIf.getName().displayValue);
    }

    public static String getNetIfDescriptionInline(String nodeName, String netIfName)
    {
        return "netInterface '" + netIfName + "' on node '" + nodeName + "'";
    }

    private ResponseContext makeNetIfContext(
        Peer peer,
        ApiOperation operation,
        String nodeNameStr,
        String netIfNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);

        return new ResponseContext(
            peer,
            operation,
            getNetIfDescription(nodeNameStr, netIfNameStr),
            getNetIfDescriptionInline(nodeNameStr, netIfNameStr),
            ApiConsts.MASK_NET_IF,
            objRefs
        );
    }
}
