package com.linbit.linstor.core.apicallhandler;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler.getStorPoolDescriptionInline;

public abstract class AbsApiCallHandler
{
    public enum LinStorObject
    {
        NODE,
        NET_IF,
        NODE_CONN,
        RESOURCE_DEFINITION,
        RESOURCE,
        RSC_CONN,
        VOLUME_DEFINITION,
        VOLUME,
        VOLUME_CONN,
        CONTROLLER,
        STORAGEPOOL,
        STORAGEPOOL_DEFINITION,
        SNAPSHOT;

        LinStorObject()
        {
        }
    }

    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    private final CtrlObjectFactories objectFactories;

    private final Provider<TransactionMgr> transMgrProvider;

    protected final Provider<AccessContext> peerAccCtx;
    protected final Provider<Peer> peer;
    private final WhitelistProps propsWhiteList;

    protected AbsApiCallHandler(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        CtrlObjectFactories objectFactoriesRef,
        Provider<TransactionMgr> transMgrProviderRef,
        Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps propsWhiteListRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        objectFactories = objectFactoriesRef;
        transMgrProvider = transMgrProviderRef;
        peerAccCtx = peerAccCtxRef;
        peer = peerRef;
        propsWhiteList = propsWhiteListRef;
    }

    /**
     * Returns the given String as a {@link NodeName} if possible. If the String is not a valid
     * {@link NodeName} an exception is thrown.
     *
     * @param nodeNameStr
     * @return
     */
    protected final NodeName asNodeName(String nodeNameStr)
    {
        NodeName nodeName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NODE_NAME, "The given node name '%s' is invalid."
            ), invalidNameExc);
        }
        return nodeName;
    }

    /**
     * Returns the given String as a {@link NetInterfaceName} if possible. If the String is not a valid
     * {@link NetInterfaceName} an exception is thrown.
     *
     * @param netIfNameStr
     * @return
     */
    protected NetInterfaceName asNetInterfaceName(String netIfNameStr)
    {
        NetInterfaceName netInterfaceName;
        try
        {
            netInterfaceName = new NetInterfaceName(netIfNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_NAME,
                "The specified net interface name '" + netIfNameStr + "' is invalid."
            ), invalidNameExc);
        }
        return netInterfaceName;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} an exception is thrown.
     *
     * @param ipAddrStr
     * @return
     */
    protected LsIpAddress asLsIpAddress(String ipAddrStr)
    {
        LsIpAddress lsIpAddress;
        try
        {
            lsIpAddress = new LsIpAddress(ipAddrStr);
        }
        catch (InvalidIpAddressException | NullPointerException invalidIpExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_INVLD_NET_ADDR, "Failed to parse IP address")
                .setCause("The specified IP address is not valid")
                .setDetails("The specified input '" + ipAddrStr + "' is not a valid IP address.")
                .setCorrection("Specify a valid IPv4 or IPv6 address.")
                .build(),
                invalidIpExc
            );
        }
        return lsIpAddress;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} an exception is thrown.
     *
     * @param ipAddrStr
     * @return
     */
    protected TcpPortNumber asTcpPortNumber(int port)
    {
        TcpPortNumber tcpPortNumber;
        try
        {
            tcpPortNumber = new TcpPortNumber(port);
        }
        catch (Exception exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_PORT,
                "The given portNumber '" + port + "' is invalid."
            ), exc);
        }
        return tcpPortNumber;
    }

    /**
     * Returns the given String as a {@link ResourceName} if possible. If the String is not a valid
     * {@link ResourceName} an exception is thrown.
     *
     * @param rscNameStr
     * @return
     */
    protected final ResourceName asRscName(String rscNameStr)
    {
        ResourceName resourceName;
        try
        {
            resourceName = new ResourceName(rscNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_RSC_NAME,
                "The specified resource name '%s' is invalid."
            ), invalidNameExc);
        }
        return resourceName;
    }

    /**
     * Returns the given int as a {@link VolumeNumber} if possible. If the int is not a valid
     * {@link VolumeNumber} an exception is thrown.
     *
     * @param vlmNr
     * @return
     */
    protected final VolumeNumber asVlmNr(int vlmNr)
    {
        VolumeNumber volumeNumber;
        try
        {
            volumeNumber = new VolumeNumber(vlmNr);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_VLM_NR,
                "The given volume number '" + vlmNr + "' is invalid. Valid range from " + VolumeNumber.VOLUME_NR_MIN +
                    " to " + VolumeNumber.VOLUME_NR_MAX
            ), valOutOfRangeExc);
        }
        return volumeNumber;
    }

    /**
     * Returns the given String as a {@link StorPoolName} if possible. If the String is not a valid
     * {@link StorPoolName} an exception is thrown.
     *
     * @param storPoolNameStr
     * @return
     */
    protected final StorPoolName asStorPoolName(String storPoolNameStr)
    {
        StorPoolName storPoolName;
        try
        {
            storPoolName = new StorPoolName(storPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                "The given storage pool name '%s' is invalid."
            ), invalidNameExc);
        }
        return storPoolName;
    }

    /**
     * Returns the given String as a {@link SnapshotName} if possible. If the String is not a valid
     * {@link SnapshotName} an exception is thrown.
     */
    protected final SnapshotName asSnapshotName(String snapshotNameStr)
    {
        SnapshotName snapshotName;
        try
        {
            snapshotName = new SnapshotName(snapshotNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_SNAPSHOT_NAME,
                "The given snapshot name '%s' is invalid."
            ), invalidNameExc);
        }
        return snapshotName;
    }

    protected final NodeData loadNode(String nodeNameStr, boolean failIfNull)
    {
        return loadNode(asNodeName(nodeNameStr), failIfNull);
    }

    protected final NodeData loadNode(NodeName nodeName, boolean failIfNull)
    {
        NodeData node;
        try
        {
            node = objectFactories.getNodeDataFactory().getInstance(
                peerAccCtx.get(),
                nodeName,
                null,
                null,
                false,
                false
            );

            if (failIfNull && node == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_NODE,
                        "Node '" + nodeName.displayValue + "' not found."
                    )
                    .setCause("The specified node '" + nodeName.displayValue + "' could not be found in the database")
                    .setCorrection("Create a node with the name '" + nodeName.displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ApiAccessDeniedException(
                accDenied,
                "loading node '" + nodeName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExists)
        {
            throw new ImplementationError(alreadyExists);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return node;
    }

    protected final ResourceDefinitionData loadRscDfn(
        String rscNameStr,
        boolean failIfNull
    )
    {
        return loadRscDfn(asRscName(rscNameStr), failIfNull);
    }

    protected final ResourceDefinitionData loadRscDfn(
        ResourceName rscName,
        boolean failIfNull
    )
    {
        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = objectFactories.getResourceDefinitionDataFactory().load(
                peerAccCtx.get(),
                rscName
            );

            if (failIfNull && rscDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        "Resource definition '" + rscName.displayValue + "' not found."
                    )
                    .setCause("The specified resource definition '" + rscName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a resource definition with the name '" + rscName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(rscName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return rscDfn;
    }

    protected ResourceData loadRsc(String nodeName, String rscName, boolean failIfNull)
    {
        Node node = loadNode(nodeName, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);

        ResourceData rscData;
        try
        {
            rscData = objectFactories.getResourceDataFactory().getInstance(
                peerAccCtx.get(),
                rscDfn,
                node,
                null,
                null,
                false,
                false
            );
            if (rscData == null && failIfNull)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        "Resource '" + rscName + "' on node '" + nodeName + "' not found."
                    )
                    .setCause("The specified resource '" + rscName + "' on node '" + nodeName + "' could not " +
                        "be found in the database")
                    .setCorrection("Create a resource with the name '" + rscName + "' on node '" + nodeName +
                        "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading a resource caused DataAlreadyExistsException",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return rscData;
    }

    protected final SnapshotDefinitionData loadSnapshotDfn(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName
    )
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = objectFactories.getSnapshotDefinitionDataFactory().load(peerAccCtx.get(), rscDfn, snapshotName);

            if (snapshotDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                        "Snapshot '" + snapshotName.displayValue +
                            "' of resource '" + rscDfn.getName().displayValue + "' not found."
                    )
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading snapshot '" + snapshotName + "' of resource '" + rscDfn.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return snapshotDfn;
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(String storPoolNameStr, boolean failIfNull)
    {
        return loadStorPoolDfn(asStorPoolName(storPoolNameStr), failIfNull);
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(
        StorPoolName storPoolName,
        boolean failIfNull
    )
    {
        StorPoolDefinitionData storPoolDfn;
        try
        {
            storPoolDfn = objectFactories.getStorPoolDefinitionDataFactory().getInstance(
                peerAccCtx.get(),
                storPoolName,
                false,
                false
            );

            if (failIfNull && storPoolDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool definition '" + storPoolName.displayValue + "' not found."
                    )
                    .setCause("The specified storage pool definition '" + storPoolName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a storage pool definition '" + storPoolName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading storage pool definition '" + storPoolName.displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading storage pool caused dataAlreadyExists exception",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPoolDfn;
    }

    protected final StorPoolData loadStorPool(
        StorPoolDefinitionData storPoolDfn,
        NodeData node,
        boolean failIfNull
    )
    {
        StorPoolData storPool;
        try
        {
            storPool = objectFactories.getStorPoolDataFactory().getInstance(
                peerAccCtx.get(),
                node,
                storPoolDfn,
                null, // storDriverSimpleClassName
                false,
                false
            );

            if (failIfNull && storPool == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                            node.getName().displayValue + "' not found.")
                    .setCause("The specified storage pool '" + storPoolDfn.getName().displayValue +
                        "' on node '" + node.getName().displayValue + "' could not be found in the database")
                    .setCorrection("Create a storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                        node.getName().displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getStorPoolDescriptionInline(node, storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading storage pool caused dataAlreadyExists exception",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return storPool;
    }

    protected final Props getProps(Node node)
    {
        Props props;
        try
        {
            props = node.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return props;
    }

    protected final Props getProps(ResourceDefinitionData rscDfn)
    {
        Props props;
        try
        {
            props = rscDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return props;
    }

    protected final Props getProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for volume definition with number '" + vlmDfn.getVolumeNumber().value + "' " +
                    "on resource definition '" + vlmDfn.getResourceDefinition().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }


    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl}.
     * Cause, details and correction messages are left empty.
     *  @param throwable
     * @param objRefs,
     * @param apiCallRc,
     * @param controller,
     * @param accCtx,
     * @param errorMsg
     * @param retCode
     * @param errorReporter
     * @param peer
     */
    public static final void reportStatic(
        Throwable throwableRef,
        String errorMsg,
        long retCode,
        Map<String, String> objRefsRef,
        ApiCallRcImpl apiCallRcRef,
        ErrorReporter errorReporter,
        AccessContext accCtx,
        Peer peer
    )
    {
        reportStatic(
            throwableRef,
            errorMsg,
            null,
            null,
            null,
            retCode,
            objRefsRef,
            apiCallRcRef,
            errorReporter,
            accCtx,
            peer
        );
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl}.
     * This method also calls
     * {@link #addAnswerStatic(String, String, String, String, long, Map} for
     * adding an answer to the {@link ApiCallRcImpl}.
     *
     * @param throwable
     * @param errorMsg
     * @param causeMsg
     * @param detailsMsg
     * @param correctionMsg
     * @param retCode
     * @param objRefsRef
     * @param apiCallRcRef
     * @param controller
     * @param accCtx
     * @param peer
     */
    public static final void reportStatic(
        Throwable throwableRef,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode,
        Map<String, String> objRefsRef,
        ApiCallRcImpl apiCallRcRef,
        ErrorReporter errorReporter,
        AccessContext accCtx,
        Peer peer
    )
    {
        Throwable throwable = throwableRef;
        if (throwable == null)
        {
            throwable = new LinStorException(errorMsg);
        }
        errorReporter.reportError(
            throwable,
            accCtx,
            peer,
            errorMsg
        );
        addAnswerStatic(
            errorMsg,
            causeMsg,
            detailsMsg,
            correctionMsg,
            retCode,
            objRefsRef,
            apiCallRcRef
        );
    }

    /**
     * This method adds an entry to {@link ApiCallRcImpl} and reports to controller's {@link ErrorReporter}.
     * <br />
     * Note that:
     * <ul>
     *  <li>The return code (parameter {@code retCode}) is not extended
     * with the "current" object and operation masks</li>
     *  <li>The details message is not extended with the object description</li>
     * </ul>
     *
     * @param msg
     * @param cause
     * @param details
     * @param correction
     * @param retCode
     * @param objRefsRef
     */
    public static final void addAnswerStatic(
        String msg,
        String cause,
        String details,
        String correction,
        long retCode,
        Map<String, String> objRefsRef,
        ApiCallRcImpl apiCallRcRef
    )
    {

        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(retCode);
        entry.setMessage(msg);
        entry.setCause(cause);
        entry.setDetails(details);
        entry.setCorrection(correction);

        if (objRefsRef != null)
        {
            entry.putAllObjRef(objRefsRef);
        }

        apiCallRcRef.addEntry(entry);
    }

    public static void reportSuccessStatic(
        String msg,
        String details,
        long retCode,
        ApiCallRcImpl apiCallRcRef,
        Map<String, String> objsRef,
        ErrorReporter errorReporter
    )
    {
        if (apiCallRcRef != null)
        {
            addAnswerStatic(
                msg,
                null,
                details,
                null,
                retCode,
                objsRef,
                apiCallRcRef
            );
        }
        if (errorReporter != null)
        {
            errorReporter.logInfo(msg);
        }
    }

    public static String getAccDeniedMsg(AccessContext accCtx, String action)
    {
        return String.format("Identity '%s' using role: '%s' is not authorized to %s.",
            accCtx.subjectId.name.displayValue,
            accCtx.subjectRole.name.displayValue,
            action
        );
    }

    public static String getSqlMsg(String action)
    {
        return String.format(
            "A database error occured while %s.",
            action
        );
    }

    protected final void commit()
    {
        try
        {
            transMgrProvider.get().commit();
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    protected void fillProperties(
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc
    )
    {
        for (Entry<String, String> entry : sourceProps.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            boolean isAuxProp = key.startsWith(ApiConsts.NAMESPC_AUXILIARY + "/");

            // boolean isPropAllowed = true;
            boolean isPropAllowed =
                isAuxProp ||
                propsWhiteList.isAllowed(linstorObj, key, value, true);
            if (isPropAllowed)
            {
                try
                {
                    targetProps.setProp(key, value);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ApiAccessDeniedException(
                        exc,
                        "insert property '" + key + "'",
                        failAccDeniedRc
                    );
                }
                catch (InvalidKeyException exc)
                {
                    if (isAuxProp)
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid key.")
                            .setCause("The key '" + key + "' is invalid.")
                            .build(),
                            exc
                        );
                    }
                    else
                    {
                        // we tried to insert an invalid but whitelisted key
                        throw new ImplementationError(exc);
                    }
                }
                catch (InvalidValueException exc)
                {
                    if (isAuxProp)
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid value.")
                            .setCause("The value '" + value + "' is invalid.")
                            .build(),
                            exc
                        );
                    }
                    else
                    {
                        // we tried to insert an invalid but whitelisted value
                        throw new ImplementationError(exc);
                    }
                }
                catch (SQLException exc)
                {
                    throw new ApiSQLException(exc);
                }
            }
            else
            if (propsWhiteList.isKeyKnown(linstorObj, key))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property value")
                    .setCause("The value '" + value + "' is not valid for the key '" + key + "'")
                    .setDetails("The value must match '" + propsWhiteList.getRuleValue(linstorObj, key) + "'")
                    .build()
                );
            }
            else
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key")
                    .setCause("The key '" + key + "' is not whitelisted.")
                    .build()
                );
            }
        }
    }
}
