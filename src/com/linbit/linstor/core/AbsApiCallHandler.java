package com.linbit.linstor.core;

import java.sql.SQLException;

import java.util.Map;
import java.util.TreeMap;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeName;
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
    protected AccessContext apiCtx;

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
    public void close() throws Exception
    {
        rollbackIfDirty();

        currentAccCtx.set(null);
        currentPeer.set(null);
        currentApiCallType.set(null);
        currentApiCallRc.set(null);
        currentObjRefs.set(null);
        currentTransMgr.set(null);
        currentVariables.set(null);
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
            throw exc(
                invalidNameExc,
                "The given node name '%s' is invalid.",
                ApiConsts.FAIL_INVLD_NODE_NAME
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
    protected final ApiCallHandlerFailedException exc(Throwable throwable, String errorMessage, long retCode)
    {
        return exc(
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
    protected final ApiCallHandlerFailedException exc(
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
     * Similar to
     * <pre>
     *    success(msg, null);
     * </pre>
     * @param msg
     */
    protected final void success(String msg)
    {
        success(msg, null);
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
    protected final void success(String msg, String details)
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
     * Basically the same as {@link #exc(Throwable, String, long)}, but with a
     * {@link AccessDeniedException}-specific template message.
     *
     * @param accDeniedExc
     * @param action
     * @param retCode
     * @return
     */
    protected final ApiCallHandlerFailedException accDeniedExc(AccessDeniedException accDeniedExc, String action, long retCode)
    {
        AccessContext accCtx = currentAccCtx.get();
        return exc(
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
     * Basically the same as {@link #exc(Throwable, String, long)}, but with a
     * {@link SQLException}-specific template message. Uses {@link ApiConsts#FAIL_SQL}
     * as return code.
     *
     * @param sqlExc
     * @param action
     * @return
     */
    protected final ApiCallHandlerFailedException sqlExc(SQLException sqlExc, String action)
    {
        return exc(
            sqlExc,
            String.format(
                "A database error occured while %s.",
                action
            ),
            ApiConsts.FAIL_SQL
        );
    }

    /**
     * Basically the same as {@link #exc(Throwable, String, long)}, but with a
     * {@link SQLException}-specific template message. Uses {@link ApiConsts#FAIL_SQL_ROLLBACK}
     * as return code.

     * @param sqlExc
     * @param action
     * @return
     */
    protected final ApiCallHandlerFailedException sqlRollbackExc(SQLException sqlExc, String action)
    {
        return exc(
            sqlExc,
            String.format(
                "A database error occured while %s.",
                action
                ),
            ApiConsts.FAIL_SQL_ROLLBACK
        );
    }

    protected static class ApiCallHandlerFailedException extends RuntimeException
    {
        private static final long serialVersionUID = -3941266567336938181L;
    }

    protected abstract TransactionMgr createNewTransMgr() throws ApiCallHandlerFailedException;

    protected abstract void rollbackIfDirty();
}
