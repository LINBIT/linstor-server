package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

abstract class AbsApiCallHandler implements AutoCloseable
{
    protected enum ApiCallType
    {
        CREATE(ApiConsts.MASK_CRT),
        MODIFY(ApiConsts.MASK_MOD),
        DELETE(ApiConsts.MASK_DEL);

        private long opMask;

        ApiCallType(long opMaskRef)
        {
            opMask = opMaskRef;
        }
    }

    private final long objMask;

    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    protected final CtrlStltSerializer internalComSerializer;
    private final CtrlObjectFactories objectFactories;

    private final Provider<TransactionMgr> transMgrProvider;

    protected final AccessContext peerAccCtx;
    protected final Provider<Peer> peer;

    protected static ThreadLocal<ApiCallType> apiCallType = new ThreadLocal<>();
    protected static ThreadLocal<ApiCallRcImpl> apiCallRc = new ThreadLocal<>();
    private static ThreadLocal<Boolean> transMgrAutoClose = new ThreadLocal<>();
    protected static ThreadLocal<Map<String, String>> objRefs = new ThreadLocal<>();
    protected static ThreadLocal<Map<String, String>> variables = new ThreadLocal<>();

    protected AbsApiCallHandler(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        long objMaskRef,
        CtrlStltSerializer serializerRef,
        CtrlObjectFactories objectFactoriesRef,
        Provider<TransactionMgr> transMgrProviderRef,
        AccessContext peerAccCtxRef,
        Provider<Peer> peerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        objMask = objMaskRef;
        internalComSerializer = serializerRef;
        objectFactories = objectFactoriesRef;
        transMgrProvider = transMgrProviderRef;
        peerAccCtx = peerAccCtxRef;
        peer = peerRef;
    }

    protected AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRcRef,
        boolean autoCloseTransMgr,
        Map<String, String> objRefsRef,
        Map<String, String> vars
    )
    {
        apiCallType.set(type);
        apiCallRc.set(apiCallRcRef);
        transMgrAutoClose.set(autoCloseTransMgr);
        objRefs.set(objRefsRef);
        variables.set(vars);
        return this;
    }

    @Override
    public void close()
    {
        if (transMgrAutoClose.get())
        {
            rollbackIfDirty();
        }
    }

    /**
     * Returns the given String as a {@link NodeName} if possible. If the String is not a valid
     * {@link NodeName} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param nodeNameStr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected final NodeName asNodeName(String nodeNameStr) throws ApiCallHandlerFailedException
    {
        NodeName nodeName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given node name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_NODE_NAME
            );
        }
        return nodeName;
    }

    /**
     * Returns the given String as a {@link NetInterfaceName} if possible. If the String is not a valid
     * {@link NetInterfaceName} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param netIfNameStr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected NetInterfaceName asNetInterfaceName(String netIfNameStr) throws ApiCallHandlerFailedException
    {
        NetInterfaceName netInterfaceName;
        try
        {
            netInterfaceName = new NetInterfaceName(netIfNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The specified net interface name '" + netIfNameStr + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_NAME
            );
        }
        return netInterfaceName;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param ipAddrStr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected LsIpAddress asLsIpAddress(String ipAddrStr) throws ApiCallHandlerFailedException
    {
        LsIpAddress lsIpAddress;
        try
        {
            lsIpAddress = new LsIpAddress(ipAddrStr);
        }
        catch (InvalidIpAddressException | NullPointerException invalidIpExc)
        {
            throw asExc(
                invalidIpExc,
                getObjectDescriptionInlineFirstLetterCaps() +
                    getAction(" creation", " modification", " deletion") +
                    "failed.",
                "The specified IP address is not valid",
                "The specified input '" + ipAddrStr + "' is not a valid IP address.",
                "Specify a valid IPv4 or IPv6 address.",
                ApiConsts.FAIL_INVLD_NET_ADDR
            );
        }
        return lsIpAddress;
    }

    /**
     * Returns the given String as a {@link LsIpAddress} if possible. If the String is not a valid
     * {@link LsIpAddress} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param ipAddrStr
     * @return
     * @throws ApiCallHandlerFailedException
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
            throw asExc(
                exc,
                "The given portNumber '" + port + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_PORT
            );
        }
        return tcpPortNumber;
    }

    /**
     * Returns the given String as a {@link ResourceName} if possible. If the String is not a valid
     * {@link ResourceName} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param rscNameStr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected final ResourceName asRscName(String rscNameStr) throws ApiCallHandlerFailedException
    {
        ResourceName resourceName;
        try
        {
            resourceName = new ResourceName(rscNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given resource name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_RSC_NAME
            );
        }
        return resourceName;
    }

    /**
     * Returns the given int as a {@link VolumeNumber} if possible. If the int is not a valid
     * {@link VolumeNumber} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param vlmNr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected final VolumeNumber asVlmNr(int vlmNr) throws ApiCallHandlerFailedException
    {
        VolumeNumber volumeNumber;
        try
        {
            volumeNumber = new VolumeNumber(vlmNr);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            throw asExc(
                valOutOfRangeExc,
                "The given volume number '" + vlmNr + "' is invalid. Valid range from " + VolumeNumber.VOLUME_NR_MIN +
                " to " + VolumeNumber.VOLUME_NR_MAX,
                ApiConsts.FAIL_INVLD_VLM_NR
            );
        }
        return volumeNumber;
    }

    /**
     * Returns the given String as a {@link StorPoolName} if possible. If the String is not a valid
     * {@link StorPoolName} the thrown exception is reported to controller's {@link ErrorReporter} and
     * the current {@link ApiCallRc} and an {@link ApiCallHandlerFailedException} is thrown.
     *
     * @param storPoolNameStr
     * @return
     * @throws ApiCallHandlerFailedException
     */
    protected final StorPoolName asStorPoolName(String storPoolNameStr) throws ApiCallHandlerFailedException
    {
        StorPoolName storPoolName;
        try
        {
            storPoolName = new StorPoolName(storPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given storage pool name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME
            );
        }
        return storPoolName;
    }

    /**
     * Depending on the current {@link ApiCallType}, this method returns the first parameter when the
     * current type is {@link ApiCallType#CREATE}, the second for {@link ApiCallType#MODIFY} or the third
     * for {@link ApiCallType#DELETE}. The default case throws an {@link ImplementationError}.
     *
     * @param crtAction
     * @param modAction
     * @param delAction
     * @return
     */
    protected final String getAction(String crtAction, String modAction, String delAction)
    {
        return getAction(crtAction, modAction, delAction, apiCallType.get());
    }

    /**
     * Depending on the parameter apiCallType, this method returns the first parameter when the
     * apiCallType is {@link ApiCallType#CREATE}, the second for {@link ApiCallType#MODIFY} or the third
     * for {@link ApiCallType#DELETE}. The default case throws an {@link ImplementationError}.
     *
     * @param crtAction
     * @param modAction
     * @param delAction
     * @param apiCallTypeRef
     * @return
     */
    protected final String getAction(String crtAction, String modAction, String delAction, ApiCallType apiCallTypeRef)
    {
        String retStr;
        switch (apiCallTypeRef)
        {
            case CREATE:
                retStr = crtAction;
                break;
            case DELETE:
                retStr = delAction;
                break;
            case MODIFY:
                retStr = modAction;
                break;
            default:
                throw new ImplementationError(
                    "Unknown api call type: " + apiCallTypeRef,
                    null
                );
        }
        return retStr;
    }

    protected final NodeData loadNode(String nodeNameStr, boolean failIfNull) throws ApiCallHandlerFailedException
    {
        return loadNode(asNodeName(nodeNameStr), failIfNull);
    }

    protected final NodeData loadNode(NodeName nodeName, boolean failIfNull) throws ApiCallHandlerFailedException
    {
        NodeData node;
        try
        {
            node = objectFactories.getNodeDataFactory().getInstance(
                peerAccCtx,
                nodeName,
                null,
                null,
                false,
                false
            );

            if (failIfNull && node == null)
            {
                throw asExc(
                    null,
                    "Node '" + nodeName.displayValue + "' not found.",
                    "The specified node '" + nodeName.displayValue + "' could not be found in the database",
                    null, // details
                    "Create a node with the name '" + nodeName.displayValue + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_NODE
                );
            }
        }
        catch (AccessDeniedException accDenied)
        {
            throw asAccDeniedExc(
                accDenied,
                "loading node '" + nodeName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExists)
        {
            throw asImplError(alreadyExists);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                getAction("creating", "modifying", "deleting") + " node '" + nodeName.displayValue + "'"
            );
        }
        return node;
    }

    protected final ResourceDefinitionData loadRscDfn(
        String rscNameStr,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        return loadRscDfn(asRscName(rscNameStr), failIfNull);
    }

    protected final ResourceDefinitionData loadRscDfn(
        ResourceName rscName,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = objectFactories.getResourceDefinitionDataFactory().load(rscName);

            if (failIfNull && rscDfn == null)
            {
                throw asExc(
                    null, // throwable
                    "Resource definition '" + rscName.displayValue + "' not found.", // error msg
                    "The specified resource definition '" + rscName.displayValue +
                        "' could not be found in the database", // cause
                    null, // details
                    "Create a resource definition with the name '" + rscName.displayValue + "' first.", // correction
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN
                );
            }

        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "loading resource definition '" + rscName.displayValue + "'.");
        }
        return rscDfn;
    }

    protected ResourceData loadRsc(String nodeName, String rscName, boolean failIfNull)
        throws ApiCallHandlerFailedException
    {
        Node node = loadNode(nodeName, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);

        ResourceData rscData;
        try
        {
            rscData = objectFactories.getResourceDataFactory().getInstance(
                peerAccCtx,
                rscDfn,
                node,
                null,
                null,
                false,
                false
            );
            if (rscData == null && failIfNull)
            {
                throw asExc(
                    null,
                    "Resource '" + rscName + "' on node '" + nodeName + "' not found.",
                    "The specified resource '" + rscName + "' on node '" + nodeName + "' could not " +
                        "be found in the database",
                    null, // details
                    "Create a resource with the name '" + rscName + "' on node '" + nodeName + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'.",
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
            throw asSqlExc(
                sqlExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'."
            );
        }
        return rscData;
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(String storPoolNameStr, boolean failIfNull)
        throws ApiCallHandlerFailedException
    {
        return loadStorPoolDfn(asStorPoolName(storPoolNameStr), failIfNull);
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(
        StorPoolName storPoolName,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        StorPoolDefinitionData storPoolDfn;
        try
        {
            storPoolDfn = objectFactories.getStorPoolDefinitionDataFactory().getInstance(
                peerAccCtx,
                storPoolName,
                false,
                false
            );

            if (failIfNull && storPoolDfn == null)
            {
                throw asExc(
                    null,
                    "Storage pool definition '" + storPoolName.displayValue + "' not found.",
                    "The specified storage pool definition '" + storPoolName.displayValue +
                        "' could not be found in the database",
                    null, // details
                    "Create a storage pool definition '" + storPoolName.displayValue + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
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
            throw asSqlExc(
                sqlExc,
                "loading storage pool definition '" + storPoolName.displayValue + "'"
            );
        }
        return storPoolDfn;
    }

    protected final StorPoolData loadStorPool(
        StorPoolDefinitionData storPoolDfn,
        NodeData node,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        StorPoolData storPool;
        try
        {
            storPool = objectFactories.getStorPoolDataFactory().getInstance(
                peerAccCtx,
                node,
                storPoolDfn,
                null, // storDriverSimpleClassName
                false,
                false
            );

            if (failIfNull && storPool == null)
            {
                throw asExc(
                    null,
                    "Storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                        node.getName().displayValue + "' not found.",
                    "The specified storage pool '" + storPoolDfn.getName().displayValue +
                        "' on node '" + node.getName().displayValue + "' could not be found in the database",
                    null, // details
                    "Create a storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                            node.getName().displayValue + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading " + getObjectDescriptionInline(),
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
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescriptionInline()
            );
        }
        return storPool;
    }

    protected final Props getProps(Node node) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = node.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for node '" + node.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return props;
    }

    protected final Props getProps(ResourceDefinitionData rscDfn) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = rscDfn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for resource definition '" + rscDfn.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return props;
    }

    protected final Props getProps(VolumeDefinition vlmDfn) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for volume definition with number '" + vlmDfn.getVolumeNumber().value + "' " +
                "on resource definition '" + vlmDfn.getResourceDefinition().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the current
     * {@link ApiCallRc} and returns an {@link ApiCallHandlerFailedException}. This returned exception
     * is expected to be thrown by the caller.
     *
     * Example:
     *
     * <pre>
     * try
     * {
     *     return ...
     * }
     * catch (Exception exc)
     * {
     *     throw exc(exc, "errorMessage", 42);
     * }
     * </pre>
     * Without throwing an exception in the catch-clause the compiler would detect an execution-path
     * with a missing return statement. This would be no different if the method exc would throw the
     * newly generated {@link ApiCallHandlerFailedException}.
     *
     * @param throwable
     * @param errorMessage
     * @param retCode
     * @return
     */
    protected final ApiCallHandlerFailedException asExc(Throwable throwable, String errorMessage, long retCode)
    {
        return asExc(
            throwable,
            errorMessage,
            throwable == null ? null : throwable.getMessage(),
            null,
            null,
            retCode
        );
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the current
     * {@link ApiCallRc} and returns an {@link ApiCallHandlerFailedException}. This returned exception
     * is expected to be thrown by the caller.
     *
     * Example:
     *
     * <pre>
     * try
     * {
     *     return ...
     * }
     * catch (Exception exc)
     * {
     *     throw asExc(exc, "errorMessage", 42);
     * }
     * </pre>
     * Without throwing an exception in the catch-clause the compiler would detect an execution-path
     * with a missing return statement. This would be no different if the method exc would throw the
     * newly generated {@link ApiCallHandlerFailedException}.
     *
     * @param throwable
     * @param errorMessage
     * @param causeMsg
     * @param detailsMsg
     * @param correctionMsg
     * @param retCode
     * @return
     */
    protected final ApiCallHandlerFailedException asExc(
        Throwable throwable,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode
    )
        throws ApiCallHandlerFailedException
    {
        report(throwable, errorMsg, causeMsg, detailsMsg, correctionMsg, retCode);
        return new ApiCallHandlerFailedException();
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the current
     * {@link ApiCallRc}.
     *
     * @param throwable
     * @param errorMessage
     * @param retCode
     */
    protected final void report(Throwable throwable, String errorMessage, long retCode)
    {
        report(
            throwable,
            errorMessage,
            throwable == null ? null : throwable.getMessage(),
            null,
            null,
            retCode
        );
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the current
     * {@link ApiCallRc}.
     *
     * @param throwable
     * @param errorMsg
     * @param causeMsg
     * @param detailsMsg
     * @param correctionMsg
     * @param retCode
     */
    protected final void report(
        Throwable throwableRef,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode
    )
    {
        Throwable throwable = throwableRef;
        if (throwable == null)
        {
            throwable = new LinStorException(errorMsg);
        }
        errorReporter.reportError(
            throwable,
            peerAccCtx,
            peer.get(),
            errorMsg
        );
        addAnswer(errorMsg, causeMsg, detailsMsg, correctionMsg, retCode);
    }

    /**
     * Adds a new {@link ApiCallRcEntry} to the current {@link ApiCallRc}.
     *
     * @param msg
     * @param retCode
     */
    protected final void addAnswer(String msg, long retCode)
    {
        addAnswer(msg, null, null, null, retCode);
    }

    /**
     * Adds a new {@link ApiCallRcEntry} to the current {@link ApiCallRc}.
     * @param msg
     * @param cause
     * @param detailsRef
     * @param correction
     * @param retCode
     */
    protected final void addAnswer(
        String msg,
        String cause,
        String detailsRef,
        String correction,
        long retCode
    )
    {
        if (apiCallRc != null)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(retCode | apiCallType.get().opMask | objMask);
            entry.setMessageFormat(msg);
            entry.setCauseFormat(cause);

            String objDescription = getObjectDescription();

            String details = detailsRef;
            if (details == null)
            {
                details = objDescription;
            }
            else
            {
                details += "\n" + objDescription;
            }

            entry.setDetailsFormat(details);
            entry.setCorrectionFormat(correction);

            Map<String, String> objsRef = objRefs.get();
            if (objsRef != null)
            {
                entry.putAllObjRef(objsRef);
            }
            if (variables != null)
            {
                entry.putAllVariables(variables.get());
            }

            apiCallRc.get().addEntry(entry);
        }
    }


    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl} with a generated error message.
     * This method can be called from the catch / finally section after the try-with-resource.
     * @param exc
     * @param type
     * @param objDescr
     * @param objRefsRef
     * @param variablesRef
     * @param apiCallRcRef
     */
    protected void reportStatic(
        Throwable exc,
        ApiCallType type,
        String objDescr,
        Map<String, String> objRefsRef,
        Map<String, String> variablesRef,
        ApiCallRcImpl apiCallRcRef
    )
    {
        String errorType;
        long retCode = objMask | type.opMask;
        if (exc instanceof ImplementationError)
        {
            errorType = "implementation error";
            retCode |= ApiConsts.FAIL_IMPL_ERROR;
        }
        else
        {
            errorType = "unknown exception";
            retCode |= ApiConsts.FAIL_UNKNOWN_ERROR;
        }

        reportStatic(
            exc,
            getAction("Creation", "Modification", "Deletion", type) + " of " + objDescr + " failed due to an " +
            errorType + ".",
            retCode,
            objRefsRef,
            variablesRef,
            apiCallRcRef,
            errorReporter,
            peerAccCtx,
            peer.get()
        );
    }


    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl}.
     * The only difference between this method and {@link #report(Throwable, String, long)}
     * is that this method does not access non-static variables. This method also calls
     * {@link #addAnswerStatic(String, String, String, String, long, Map, Map, ApiCallRcImpl)} for
     * adding an answer to the {@link ApiCallRcImpl} (cause, details and correction messages are filled
     * with null values).
     *  @param throwable
     * @param objRefs,
     * @param apiCallRc,
     * @param controller,
     * @param accCtx,
     * @param errorMsg
     * @param retCode
     * @param variablesRef
     * @param errorReporter
     * @param peer
     */
    protected static final void reportStatic(
        Throwable throwableRef,
        String errorMsg,
        long retCode,
        Map<String, String> objRefsRef,
        Map<String, String> variablesRef,
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
            null,
            null,
            null,
            retCode,
            objRefsRef,
            variablesRef,
            apiCallRcRef
        );
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl}.
     * The only difference between this method and {@link #report(Throwable, String, String, String, String, long)}
     * is that this method does not access non-static variables. This method also calls
     * {@link #addAnswerStatic(String, String, String, String, long, Map, Map, ApiCallRcImpl)} for
     * adding an answer to the {@link ApiCallRcImpl}.
     *
     * @param throwable
     * @param errorMsg
     * @param causeMsg
     * @param detailsMsg
     * @param correctionMsg
     * @param retCode
     * @param objRefsRef
     * @param variablesRef
     * @param apiCallRcRef
     * @param controller
     * @param accCtx
     * @param peer
     */
    protected static final void reportStatic(
        Throwable throwableRef,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode,
        Map<String, String> objRefsRef,
        Map<String, String> variablesRef,
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
            variablesRef,
            apiCallRcRef
        );
    }

    /**
     * This method adds an entry to {@link ApiCallRcImpl} and reports to controller's {@link ErrorReporter}.
     * The main difference between this method and {@link #addAnswer(String, String, String, String, long)}
     * is that this method does not use any non-static variables. <br />
     * <br />
     * This implies following limitations:
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
     * @param variablesRef
     */
    protected static final void addAnswerStatic(
        String msg,
        String cause,
        String details,
        String correction,
        long retCode,
        Map<String, String> objRefsRef,
        Map<String, String> variablesRef,
        ApiCallRcImpl apiCallRcRef
    )
    {

        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(retCode);
        entry.setMessageFormat(msg);
        entry.setCauseFormat(cause);
        entry.setDetailsFormat(details);
        entry.setCorrectionFormat(correction);

        if (objRefsRef != null)
        {
            entry.putAllObjRef(objRefsRef);
        }
        if (variablesRef != null)
        {
            entry.putAllVariables(variablesRef);
        }

        apiCallRcRef.addEntry(entry);
    }

    /**
     * Generates a default success report depending on the current {@link ApiCallType}.
     * Inserts the given UUID as details-message
     * @param uuid
     */
    protected final void reportSuccess(UUID uuid)
    {
        objRefs.get().put(ApiConsts.KEY_UUID, uuid.toString());

        switch (apiCallType.get())
        {
            case CREATE:
                reportSuccess(
                    "New " + getObjectDescriptionInline() + " created.",
                    getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + uuid.toString()
                );
                break;
            case DELETE:
                reportSuccess(
                    getObjectDescriptionInlineFirstLetterCaps() + " deleted.",
                    getObjectDescriptionInlineFirstLetterCaps() + " UUID was: " + uuid.toString()
                );
                break;
            case MODIFY:
                reportSuccess(
                    getObjectDescriptionInlineFirstLetterCaps() + " updated.",
                    getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + uuid.toString()
                );
                break;
            default:
                throw new ImplementationError(
                    "Unknown api call type: " + apiCallType.get(),
                    null
                );
        }
    }

    /**
     * Similar to
     * <pre>
     *    reportSuccess(msg, null);
     * </pre>
     * @param msg
     */
    protected final void reportSuccess(String msg)
    {
        reportSuccess(msg, null);
    }

    /**
     * Adds a success {@link ApiCallRcEntry} to the current {@link ApiCallRc} and reports
     * to the controller's {@link ErrorReporter}.
     * @param msg
     */
    protected final void reportSuccess(String msg, String details)
    {
        long baseRetCode;
        switch (apiCallType.get())
        {
            case CREATE:
                baseRetCode = ApiConsts.MASK_CRT | ApiConsts.CREATED;
                break;
            case DELETE:
                baseRetCode = ApiConsts.MASK_DEL | ApiConsts.DELETED;
                break;
            case MODIFY:
                baseRetCode = ApiConsts.MASK_MOD | ApiConsts.MODIFIED;
                break;
            default:
                throw new ImplementationError(
                    "Unknown api call type: " + apiCallType.get(),
                    null
                );
        }
        reportSuccess(msg, details, baseRetCode | objMask);
    }


    protected void reportSuccess(String msg, String details, long retCode)
    {
        reportSuccessStatic(
            msg,
            details,
            retCode,
            apiCallRc.get(),
            objRefs.get(),
            variables.get(),
            errorReporter
        );
    }


    public static void reportSuccessStatic(
        String msg,
        String details,
        long retCode,
        ApiCallRcImpl apiCallRcRef,
        Map<String, String> objsRef,
        Map<String, String> variablesRef,
        ErrorReporter errorReporter
    )
    {
        if (apiCallRcRef != null)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();

            entry.setReturnCodeBit(retCode);
            entry.setMessageFormat(msg);
            entry.setDetailsFormat(details);

            if (objsRef != null)
            {
                entry.putAllObjRef(objsRef);
            }
            if (variablesRef != null)
            {
                entry.putAllVariables(variablesRef);
            }
            apiCallRcRef.addEntry(entry);
        }
        if (errorReporter != null)
        {
            errorReporter.logInfo(msg);
        }
    }

    /**
     * Basically the same as {@link #asExc(Throwable, String, long)}, but with a
     * {@link AccessDeniedException}-specific template message.
     *
     * @param accDeniedExc
     * @param action
     * @param retCode
     * @return
     */
    protected final ApiCallHandlerFailedException asAccDeniedExc(
        AccessDeniedException accDeniedExc,
        String action,
        long retCode
    )
    {
        return asExc(
            accDeniedExc,
            getAccDeniedMsg(peerAccCtx, action),
            retCode
        );
    }

    public static String getAccDeniedMsg(AccessContext accCtx, String action)
    {
        return String.format("Identity '%s' using role: '%s' is not authorized to %s.",
            accCtx.subjectId.name.displayValue,
            accCtx.subjectRole.name.displayValue,
            action
        );
    }

    /**
     * Basically the same as {@link #asExc(Throwable, String, long)}, but with a
     * {@link SQLException}-specific template message. Uses {@link ApiConsts#FAIL_SQL}
     * as return code.
     *
     * @param sqlExc
     * @param action
     * @return
     */
    protected final ApiCallHandlerFailedException asSqlExc(SQLException sqlExc, String action)
    {
        return asExc(
            sqlExc,
            getSqlMsg(action),
            ApiConsts.FAIL_SQL
        );
    }

    public static String getSqlMsg(String action)
    {
        return String.format(
            "A database error occured while %s.",
            action
        );
    }

    /**
     * Basically the same as {@link #asExc(Throwable, String, long)}, but with a
     * {@link SQLException}-specific template message. Uses {@link ApiConsts#FAIL_SQL_ROLLBACK}
     * as return code.

     * @param sqlExc
     * @param action
     * @return
     */
    protected final ApiCallHandlerFailedException asSqlRollbackExc(SQLException sqlExc, String action)
    {
        return asExc(
            sqlExc,
            getSqlMsg(action),
            ApiConsts.FAIL_SQL_ROLLBACK
        );
    }

    protected final ApiCallHandlerFailedException asImplError(Throwable throwableRef)
    {
        Throwable throwable = throwableRef;
        if (!(throwable instanceof ImplementationError))
        {
            throwable = new ImplementationError(throwable);
        }

        throw asExc(
            throwable,
            "The " + getObjectDescriptionInline() + " could not be " +
                getAction("created", "deleted", "modified") + " due to an implementation error",
            ApiConsts.FAIL_IMPL_ERROR
        );
    }

    protected final void commit() throws ApiCallHandlerFailedException
    {
        try
        {
            transMgrProvider.get().commit();
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "committing " + getAction("creation", "modification", "deletion") +
                " of " + getObjectDescriptionInline()
            );
        }
    }

    protected void rollbackIfDirty()
    {
        TransactionMgr transMgr = transMgrProvider.get();
        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    report(
                        sqlExc,
                        "A database error occured while trying to rollback the " +
                        getAction("creation", "modification", "deletion") +
                        " of " + getObjectDescriptionInline() + ".",
                        ApiConsts.FAIL_SQL_ROLLBACK
                    );
                }
            }
        }
    }

    /**
     * This method depends on a valid instance of {@link InterComSerializer}. If none was given
     * at construction time an {@link ImplementationError} is thrown.
     * @return
     */
    protected final void updateSatellites(Node node)
    {
        if (internalComSerializer == null)
        {
            throw new ImplementationError(
                "UpdateSatellites(Node) was called without providing a valid node serializer",
                null
            );
        }

        try
        {
            Map<NodeName, Node> nodesToContact = new TreeMap<>();
            nodesToContact.put(node.getName(), node);
            for (Resource rsc : node.streamResources(apiCtx).collect(toList()))
            {
                ResourceDefinition rscDfn = rsc.getDefinition();
                Iterator<Resource> allRscsIterator = rscDfn.iterateResource(apiCtx);
                while (allRscsIterator.hasNext())
                {
                    Resource allRsc = allRscsIterator.next();
                    nodesToContact.put(allRsc.getAssignedNode().getName(), allRsc.getAssignedNode());
                }
            }

            byte[] changedMessage = internalComSerializer
                .builder(InternalApiConsts.API_CHANGED_NODE, 0)
                .changedNode(
                    node.getUuid(),
                    node.getName().displayValue
                )
                .build();
            for (Node nodeToContact : nodesToContact.values())
            {
                Peer satellitePeer = nodeToContact.getPeer(apiCtx);
                if (satellitePeer.isConnected() && !fullSyncFailed(satellitePeer))
                {
                    satellitePeer.sendMessage(changedMessage);
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw asImplError(implError);
        }
    }

    /**
     * This method depends on a valid instance of {@link InterComSerializer}. If none was given
     * at construction time an {@link ImplementationError} is thrown.
     * @return
     */
    protected final void updateSatellites(Resource rsc)
    {
        updateSatellites(rsc.getDefinition());
    }

    /**
     * This method depends on a valid instance of {@link InterComSerializer}. If none was given
     * at construction time an {@link ImplementationError} is thrown.
     * @return
     */
    protected final void updateSatellites(ResourceDefinition rscDfn)
    {
        if (internalComSerializer == null)
        {
            throw new ImplementationError(
                "UpdateSatellites(ResourceDefinition) was called without providing a valid resource serializer",
                null
            );
        }
        try
        {
            // notify all peers that (at least one of) their resource has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Peer currentPeer = currentRsc.getAssignedNode().getPeer(apiCtx);

                boolean connected = currentPeer.isConnected();
                if (connected)
                {
                    if (!fullSyncFailed(currentPeer))
                    {
                        connected = currentPeer.sendMessage(
                            internalComSerializer
                            .builder(InternalApiConsts.API_CHANGED_RSC, 0)
                            .changedResource(
                                currentRsc.getUuid(),
                                currentRsc.getDefinition().getName().displayValue
                            )
                            .build()
                        );
                    }
                }
                if (!connected)
                {
                    String nodeName = currentRsc.getAssignedNode().getName().displayValue;
                    String details = String.format(
                        "The satellite was added and the controller tries to (re-) establish connection to it. " +
                        "The controller queued the %s of the resource and as soon the satellite is connected, " +
                        "it will receive this update.",
                        getAction("creation", "modification", "deletion")
                    );
                    addAnswer(
                        "No active connection to satellite '" + nodeName + "'",
                        null, // cause
                        details,
                        null, // correction
                        ApiConsts.WARN_NOT_CONNECTED
                    );
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw asImplError(implError);
        }
    }

    /**
     * This method depends on a valid instance of {@link InterComSerializer}. If none was given
     * at construction time an {@link ImplementationError} is thrown.
     * @return
     */
    protected final void updateSatellite(StorPool storPool)
    {
        if (internalComSerializer == null)
        {
            throw new ImplementationError(
                "UpdateSatellites(StorPool) was called without providing a valid StorPool serializer",
                null
            );
        }
        try
        {
            Peer satellitePeer = storPool.getNode().getPeer(apiCtx);
            boolean connected = satellitePeer.isConnected();
            if (connected)
            {
                if (!fullSyncFailed(satellitePeer))
                {
                    connected = satellitePeer.sendMessage(
                        internalComSerializer
                        .builder(InternalApiConsts.API_CHANGED_STOR_POOL, 0)
                        .changedStorPool(
                            storPool.getUuid(),
                            storPool.getName().displayValue
                        )
                        .build()
                    );
                }
            }
            if (!connected)
            {
                addAnswer(
                    "No active connection to satellite '" + storPool.getNode().getName().displayValue + "'",
                    null, // cause
                    "The satellite was added and the controller tries to (re-) establish connection to it." +
                    "The controller stored the new StorPool and as soon the satellite is connected, it will " +
                    "receive this update.",
                    null, // correction
                    ApiConsts.WARN_NOT_CONNECTED
                );
            }
        }
        catch (AccessDeniedException implError)
        {
            throw asImplError(implError);
        }
    }

    /**
     * Checks and returns if the satellite reported a fullSyncFail.
     * If true, an answer is added to report the client about the issue
     *
     * @param satellite
     * @return The result of <code>satellite.hasFullSyncFailed();</code>
     */
    private boolean fullSyncFailed(Peer satellite)
    {
        boolean ret = satellite.hasFullSyncFailed();
        if (ret)
        {
            addAnswer(
                "Satellite reported an error during fullSync. This change will NOT be " +
                    "delivered to satellte '" + satellite.getNode().getName().displayValue +
                    "' until the error is resolved. Reconnect the satellite to the controller " +
                    "to remove this blockade.",
                ApiConsts.WARN_STLT_NOT_UPDATED
            );
        }
        return ret;
    }

    protected String getObjectDescriptionInlineFirstLetterCaps()
    {
        String objDescrLower = getObjectDescriptionInline();
        return objDescrLower.substring(0, 1).toUpperCase() + objDescrLower.substring(1);
    }

    protected abstract String getObjectDescription();

    protected abstract String getObjectDescriptionInline();

    protected static class ApiCallHandlerFailedException extends RuntimeException
    {
        private static final long serialVersionUID = -3941266567336938181L;
    }
}
