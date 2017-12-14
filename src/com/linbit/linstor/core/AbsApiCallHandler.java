package com.linbit.linstor.core;

import java.sql.SQLException;

import java.util.Map;
import java.util.TreeMap;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

abstract class AbsApiCallHandler implements AutoCloseable
{
    protected enum ApiCallType
    {
        CREATE (ApiConsts.MASK_CRT),
        MODIFY (ApiConsts.MASK_MOD),
        DELETE (ApiConsts.MASK_DEL);

        private long opMask;

        private ApiCallType(long opMask)
        {
            this.opMask = opMask;
        }
    }
    private final long objMask;

    protected final ThreadLocal<AccessContext> currentAccCtx = new ThreadLocal<>();
    protected final ThreadLocal<Peer> currentPeer = new ThreadLocal<>();
    protected final ThreadLocal<ApiCallType> currentApiCallType = new ThreadLocal<>();
    protected final ThreadLocal<ApiCallRcImpl> currentApiCallRc = new ThreadLocal<>();
    protected final ThreadLocal<TransactionMgr> currentTransMgr = new ThreadLocal<>();
    protected final ThreadLocal<Map<String, String>> currentObjRefs = new ThreadLocal<>();
    protected final ThreadLocal<Map<String, String>> currentVariables = new ThreadLocal<>();
    protected final Controller controller;
    protected final AccessContext apiCtx;

    private ThreadLocal<?>[] customThreadLocals;

    protected AbsApiCallHandler(
        Controller controllerRef,
        AccessContext apiCtxRef,
        long objMaskRef
    )
    {
        controller = controllerRef;
        apiCtx = apiCtxRef;
        objMask = objMaskRef;
    }

    public void setNullOnAutoClose(ThreadLocal<?>... customThreadLocals)
    {
        this.customThreadLocals = customThreadLocals;

    }

    protected AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr
    )
    {
        currentAccCtx.set(accCtx);
        currentPeer.set(peer);
        currentApiCallType.set(type);
        currentApiCallRc.set(apiCallRc);
        if (transMgr == null)
        {
            currentTransMgr.set(createNewTransMgr());
        }
        else
        {
            currentTransMgr.set(transMgr);
        }
        currentObjRefs.set(new TreeMap<String, String>());
        currentVariables.set(new TreeMap<String, String>());
        return this;
    }

    @Override
    public void close()
    {
        rollbackIfDirty();

        currentAccCtx.set(null);
        currentPeer.set(null);
        currentApiCallType.set(null);
        currentApiCallRc.set(null);
        currentObjRefs.set(null);
        TransactionMgr transMgr = currentTransMgr.get();
        currentTransMgr.set(null);
        currentVariables.set(null);

        if (customThreadLocals != null)
        {
            for (ThreadLocal<?> customThreadLocal : customThreadLocals)
            {
                customThreadLocal.set(null);
            }
        }
        if (transMgr != null)
        {
            controller.dbConnPool.returnConnection(transMgr);
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
        try
        {
            return new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given node name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_NODE_NAME
            );
        }
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
        try
        {
            return new ResourceName(rscNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given resource name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_RSC_NAME
            );
        }
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
    protected final VolumeNumber asVlmNr(int vlmNr)
    {
        try
        {
            return new VolumeNumber(vlmNr);

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
    protected final StorPoolName asStorPoolName(String storPoolNameStr)
    {
        try
        {
            return new StorPoolName(storPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The given storage pool name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME
            );
        }
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
        switch (currentApiCallType.get())
        {
            case CREATE:
                return crtAction;
            case DELETE:
                return delAction;
            case MODIFY:
                return modAction;
            default:
                throw new ImplementationError(
                    "Unknown api call type: " + currentApiCallType.get(),
                    null
                );
        }
    }

    protected final NodeData loadNode(String nodeNameStr) throws ApiCallHandlerFailedException
    {
        return loadNode(asNodeName(nodeNameStr));
    }

    protected final NodeData loadNode(NodeName nodeName) throws ApiCallHandlerFailedException
    {
        try
        {
            return NodeData.getInstance(
                currentAccCtx.get(),
                nodeName,
                null,
                null,
                currentTransMgr.get(),
                false,
                false
            );
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
    }

    protected final ResourceDefinitionData loadRscDfn(String rscNameStr) throws ApiCallHandlerFailedException
    {
        return loadRscDfn(asRscName(rscNameStr));
    }

    protected final ResourceDefinitionData loadRscDfn(ResourceName rscName) throws ApiCallHandlerFailedException
    {
        try
        {
            return ResourceDefinitionData.getInstance(
                currentAccCtx.get(),
                rscName,
                null, // port
                null, // flags
                null, // secret
                null, // transType
                currentTransMgr.get(),
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "load resource definition '" + rscName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading resource definition caused DataAlreadyExistsExc.",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "loading resource definition '" + rscName.displayValue + "'.");
        }
    }


    protected final StorPoolDefinitionData loadStorPoolDfn(String storPoolNameStr) throws ApiCallHandlerFailedException
    {
        return loadStorPoolDfn(asStorPoolName(storPoolNameStr));
    }

    protected final StorPoolDefinitionData loadStorPoolDfn(StorPoolName storPoolName)
    {
        try
        {
            return StorPoolDefinitionData.getInstance(
                currentAccCtx.get(),
                storPoolName,
                currentTransMgr.get(),
                false,
                false
            );
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
     *     throw exc(exc, "errorMessage", 42);
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
        Throwable throwable,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode
    )
    {
        if (throwable == null)
        {
            throwable = new LinStorException(errorMsg);
        }
        controller.getErrorReporter().reportError(
            throwable,
            currentAccCtx.get(),
            currentPeer.get(),
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
     * @param details
     * @param correction
     * @param retCode
     */
    protected final void addAnswer(
        String msg,
        String cause,
        String details,
        String correction,
        long retCode
    )
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(retCode | currentApiCallType.get().opMask | objMask);
        entry.setMessageFormat(msg);
        entry.setCauseFormat(cause);

        String objDescription = getObjectDescription();

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

        Map<String, String> objsRef = currentObjRefs.get();
        if (objsRef != null)
        {
            entry.putAllObjRef(objsRef);
        }
        Map<String, String> variables = currentVariables.get();
        if (variables != null)
        {
            entry.putAllVariables(variables);
        }

        currentApiCallRc.get().addEntry(entry);
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
     */
    protected static final void reportStatic(
        Throwable throwable,
        String errorMsg,
        String causeMsg,
        String detailsMsg,
        String correctionMsg,
        long retCode,
        Map<String, String> objRefs,
        Map<String, String> variables,
        ApiCallRcImpl apiCallRc,
        Controller controller,
        AccessContext accCtx,
        Peer peer
    )
    {
        if (throwable == null)
        {
            throwable = new LinStorException(errorMsg);
        }
        controller.getErrorReporter().reportError(
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
            objRefs,
            variables,
            apiCallRc
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
     * @param objRefs
     * @param variables
     */
    protected static final void addAnswerStatic(
        String msg,
        String cause,
        String details,
        String correction,
        long retCode,
        Map<String, String> objRefs,
        Map<String, String> variables,
        ApiCallRcImpl apiCallRc
    )
    {

        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(retCode);
        entry.setMessageFormat(msg);
        entry.setCauseFormat(cause);
        entry.setDetailsFormat(details);
        entry.setCorrectionFormat(correction);

        if (objRefs != null)
        {
            entry.putAllObjRef(objRefs);
        }
        if (variables != null)
        {
            entry.putAllVariables(variables);
        }

        apiCallRc.addEntry(entry);
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
     * to the controller's {@link ErrorReporter}. <br />
     * <br />
     * This method uses a special masking. Instead of using the MASK_CRT, MASK_MOD or MASK_DEL
     * from {@link ApiConsts}, it uses the special {@link ApiConsts#CREATED}, {@link ApiConsts#MODIFIED}
     * and {@link ApiConsts#DELETED} combined with the object mask (Node, RscDfn, ...) <br />
     * This combination is the same as stated in the {@link ApiConsts#RC_NODE_CREATED} for example.
     *
     *
     * @param msg
     */
    protected final void reportSuccess(String msg, String details)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        long baseRetCode;
        switch (currentApiCallType.get())
        {
            case CREATE:
                baseRetCode = ApiConsts.CREATED;
                break;
            case DELETE:
                baseRetCode = ApiConsts.DELETED;
                break;
            case MODIFY:
                baseRetCode = ApiConsts.MODIFIED;
                break;
            default:
                throw new ImplementationError(
                    "Unknown api call type: " + currentApiCallType.get(),
                    null
                );
        }
        entry.setReturnCodeBit(baseRetCode | objMask);
        entry.setMessageFormat(msg);
        entry.setDetailsFormat(details);

        Map<String, String> objsRef = currentObjRefs.get();
        if (objsRef != null)
        {
            entry.putAllObjRef(objsRef);
        }
        Map<String, String> variables = currentVariables.get();
        if (variables != null)
        {
            entry.putAllVariables(variables);
        }

        currentApiCallRc.get().addEntry(entry);
        controller.getErrorReporter().logInfo(msg);
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
    protected final ApiCallHandlerFailedException asAccDeniedExc(AccessDeniedException accDeniedExc, String action, long retCode)
    {
        AccessContext accCtx = currentAccCtx.get();
        return asExc(
            accDeniedExc,
            String.format("Identity '%s' using role: '%s' is not authorized to %s.",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            ),
            retCode
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
            String.format(
                "A database error occured while %s.",
                action
            ),
            ApiConsts.FAIL_SQL
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
            String.format(
                "A database error occured while %s.",
                action
                ),
            ApiConsts.FAIL_SQL_ROLLBACK
        );
    }

    protected final ApiCallHandlerFailedException asImplError(Throwable throwable)
    {
        if (!(throwable instanceof ImplementationError))
        {
            throwable = new ImplementationError(throwable);
        }

        throw asExc(
            throwable,
            "The " + getObjectDescriptionInline() + " could not be " + getAction("created", "deleted", "modified") +
            " due to an implementation error",
            ApiConsts.FAIL_IMPL_ERROR
        );
    }

    protected TransactionMgr createNewTransMgr() throws ApiCallHandlerFailedException
    {
        try
        {
            return new TransactionMgr(controller.dbConnPool.getConnection());
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "creating a new transaction manager");
        }
    }

    protected final void commit() throws ApiCallHandlerFailedException
    {
        try
        {
            currentTransMgr.get().commit();
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
        TransactionMgr transMgr = currentTransMgr.get();
        if (transMgr != null)
        {
            try
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
            finally
            {
                controller.dbConnPool.returnConnection(transMgr);
            }
        }
    }

    protected abstract String getObjectDescription();

    protected abstract String getObjectDescriptionInline();

    protected static class ApiCallHandlerFailedException extends RuntimeException
    {
        private static final long serialVersionUID = -3941266567336938181L;
    }
}
