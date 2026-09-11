package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;

import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_ENCRYPT_TYPE;
import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_NET_PORT;
import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_NODE_TYPE;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
class CtrlNetIfApiCallHandler
{
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NetInterfaceFactory netInterfaceFactory;
    private final SatelliteConnector satelliteConnector;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final SpecialSatelliteProcessManager specStltTargetProcMgr;
    private final DynamicNumberPool specStltTargetPortPool;

    @Inject
    CtrlNetIfApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NetInterfaceFactory netInterfaceFactoryRef,
        SatelliteConnector satelliteConnectorRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        SpecialSatelliteProcessManager specStltTargetProcMgrRef,
        @Named(NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL) DynamicNumberPool specStltTargetPortPoolRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        netInterfaceFactory = netInterfaceFactoryRef;
        satelliteConnector = satelliteConnectorRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        specStltTargetProcMgr = specStltTargetProcMgrRef;
        specStltTargetPortPool = specStltTargetPortPoolRef;
    }

    public ApiCallRc createNetIf(
        String nodeNameStr,
        String netIfNameStr,
        String address,
        Integer stltPort,
        String stltEncrType,
        Boolean setActivePrm
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            netIfNameStr
        );

        final boolean setActive = setActivePrm != null ? setActivePrm : false;

        try
        {
            Node node = ctrlApiDataLoader.loadNode(nodeNameStr);

            if (node.getNodeType().isSpecial())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        FAIL_INVLD_NODE_TYPE,
                        "Only one network interface allowed for '" + node.getNodeType().name() + "' nodes" // FIXME?
                    )
                );
            }
            NetInterfaceName netIfName = LinstorParsingUtils.asNetInterfaceName(netIfNameStr);
            NetInterface netIf = createNetIf(node, netIfName, address, stltPort, stltEncrType);

            if (node.getActiveStltConn() == null || setActive)
            {
                if (stltPort != null && stltEncrType != null)
                {
                    node.getPeer().closeConnection(false);
                    node.setActiveStltConn(netIf);
                    satelliteConnector.startConnecting(node);
                }
                else
                if (setActive)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            FAIL_INVLD_NET_PORT | FAIL_INVLD_ENCRYPT_TYPE, //TODO rigth?
                            "No satellite port / encryption type set for active satellite connection"
                        )
                    );
                }
            }

            ctrlTransactionHelper.commit();

            if (node.getActiveStltConn() == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NO_STLT_CONN_DEFINED,
                    "No active satellite-connection configured on node '" + nodeNameStr + "'!"
                ).setCorrection("Create at least one netInterface with a valid PORT and COMMUNICATION-TYPE."));
            }

            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(netIf.getUuid(), getNetIfDescriptionInline(netIf)));
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(node));
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
        @Nullable String addressStr,
        @Nullable Integer stltPort,
        @Nullable String stltEncrType,
        @Nullable Boolean setActivePrm
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            netIfNameStr
        );

        try
        {
            NetInterface netIf = ctrlApiDataLoader.loadNetIf(nodeNameStr, netIfNameStr);
            Node node = netIf.getNode();
            Node.Type nodeType = netIf.getNode().getNodeType();
            @Nullable NetInterface activeStltConn = node.getActiveStltConn();
            boolean isModifyingActiveStltConn =
                activeStltConn != null && netIf.getUuid().equals(activeStltConn.getUuid());

            final boolean setActive = setActivePrm != null ? setActivePrm : false;

            // reconnect necessary if ip or port changes on the active stlt conn
            boolean needsReconnect =
                (!isModifyingActiveStltConn && setActive) ||
                (isModifyingActiveStltConn && (addressStr != null || (stltPort != null && stltEncrType != null)));

            if (needsReconnect && nodeType.isSpecial())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        FAIL_INVLD_NODE_TYPE,
                        "Modifying netinterface " + getNetIfDescriptionInline(netIf) + " failed"
                    )
                        .setCause("Changing the address of a " + nodeType + " is prohibited")
                        .build()
                );
            }

            if (addressStr != null)
            {
                setAddress(netIf, addressStr);
            }

            if (stltPort != null && stltEncrType != null)
            {
                TcpPortNumber oldPort = netIf.getStltConnPort();
                boolean needsStartProc = false;

                if (oldPort != null && stltPort != oldPort.value && nodeType.isSpecial())
                {
                    specStltTargetProcMgr.stopProcess(netIf.getNode());
                    specStltTargetPortPool.deallocate(oldPort.value);
                    specStltTargetPortPool.allocate(stltPort);
                    needsStartProc = true;
                }
                needsReconnect = setStltConn(netIf, stltPort, stltEncrType) || setActive;
                if (needsStartProc)
                {
                    specStltTargetProcMgr.startLocalSatelliteProcess(node);
                }
            }

            if (setActive)
            {
                if (netIf.isUsableAsStltConn())
                {
                    node.setActiveStltConn(netIf);
                }
                else
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            FAIL_INVLD_NET_PORT | FAIL_INVLD_ENCRYPT_TYPE,
                            "No satellite port / encryption type set for active satellite connection"
                        )
                    );
                }
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                netIf.getUuid(), getNetIfDescriptionInline(netIf)));

            if (needsReconnect)
            {
                node.getPeer().closeConnection();
                satelliteConnector.startConnecting(node);
            }
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(node));

            if (node.getActiveStltConn() == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NO_STLT_CONN_DEFINED,
                    "No active satellite-connection configured on node '" + nodeNameStr + "'!"
                ).setCorrection("Create at least one netInterface with a valid PORT and COMMUNICATION-TYPE."));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }


    public ApiCallRc deleteNetIf(
        String nodeNameStr,
        String netIfNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNetIfContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            netIfNameStr
        );

        try
        {
            @Nullable NetInterface netIf = ctrlApiDataLoader.loadNetIfOrNull(nodeNameStr, netIfNameStr);
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

                @Nullable NetInterface activeStltConn = node.getActiveStltConn();
                boolean closeConnection = activeStltConn != null && netIf.getUuid().equals(activeStltConn.getUuid());

                UUID uuid = netIf.getUuid();

                if (closeConnection)
                {
                    // updateSatellites(node);
                    // FIXME: updating a satellite is risky here.
                    // when the sending takes too long, the connection will be already closed (next statement)
                    // for now, just close the connection. once a new connection is established, the
                    // satellite gets a full sync anyways
                    node.getPeer().closeConnection();

                    // look for another net interface configured as satellite connection and set it as active
                    Iterator<NetInterface> netIfIterator = node.iterateNetInterfaces();
                    while (netIfIterator.hasNext())
                    {
                        NetInterface netInterface = netIfIterator.next();
                        if (
                            !netInterface.equals(netIf) &&
                                // netIf is going to be deleted soon, do not consider it as a replacement for itself
                                netInterface.isUsableAsStltConn()
                        )
                        {
                            node.setActiveStltConn(netInterface);
                            satelliteConnector.startConnecting(node);
                            break;
                        }
                    }
                }
                // needs to be deleted after finding a replacement to prevent AccessToDeletedDataException
                deleteNetIf(netIf);

                ctrlTransactionHelper.commit();
                responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                    uuid, getNetIfDescriptionInline(nodeNameStr, netIfNameStr)));

                // no active satellite connection configured
                if (node.getActiveStltConn() == null)
                {
                    node.getPeer().setConnectionStatus(ApiConsts.ConnectionStatus.NO_STLT_CONN);

                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_NO_STLT_CONN_DEFINED,
                        firstLetterCaps(getNetIfDescriptionInline(nodeNameStr, netIfNameStr)) +
                            " was the last connection to the satellite! \n" +
                            "To fix this, create at least one netInterface with a valid " +
                            "PORT and COMMUNICATION-TYPE."
                    ));
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private NetInterface createNetIf(
        Node node,
        NetInterfaceName netIfName,
        String address,
        Integer stltConnPort,
        String stltConnEncrType
    )
    {
        String nodeNameStr = node.getName().displayValue;
        String netIfNameStr = netIfName.displayValue;

        NetInterface netIf;
        try
        {
            TcpPortNumber port = null;
            EncryptionType type = null;
            if (stltConnPort != null && stltConnEncrType != null)
            {
                port = LinstorParsingUtils.asTcpPortNumber(stltConnPort);
                type = asEncryptionType(stltConnEncrType);
            }

            netIf = netInterfaceFactory.create(
                node,
                netIfName,
                LinstorParsingUtils.asLsIpAddress(address),
                port,
                type
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_NET_IF,
                firstLetterCaps(getNetIfDescriptionInline(nodeNameStr, netIfNameStr)) + " already exists.",
                true
            ), exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
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

    private void setAddress(NetInterface netIf, String addressStr)
    {
        try
        {
            netIf.setAddress(LinstorParsingUtils.asLsIpAddress(addressStr));
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private boolean setStltConn(NetInterface netIf, Integer stltPort, String stltEncrType)
    {
        boolean changed = false;
        try
        {
            changed = netIf.setStltConn(
                LinstorParsingUtils.asTcpPortNumber(stltPort),
                asEncryptionType(stltEncrType)
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return changed;
    }

    private void deleteNetIf(NetInterface netIf)
    {
        try
        {
            netIf.delete();
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
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
        ApiOperation operation,
        String nodeNameStr,
        String netIfNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);

        return new ResponseContext(
            operation,
            getNetIfDescription(nodeNameStr, netIfNameStr),
            getNetIfDescriptionInline(nodeNameStr, netIfNameStr),
            ApiConsts.MASK_NET_IF,
            objRefs
        );
    }
}
