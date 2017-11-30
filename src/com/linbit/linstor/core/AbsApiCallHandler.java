package com.linbit.linstor.core;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

abstract class AbsApiCallHandler implements AutoCloseable
{
    protected enum ApiCallType
    {
        CREATE, DELETE
    }

    protected final ThreadLocal<AccessContext> currentAccCtx = new ThreadLocal<>();
    protected final ThreadLocal<Peer> currentPeer = new ThreadLocal<>();
    protected final ThreadLocal<ApiCallType> currentApiCallType = new ThreadLocal<>();
    protected final ThreadLocal<ApiCallRcImpl> currentApiCallRc = new ThreadLocal<>();
    protected final ThreadLocal<TransactionMgr> currentTransMgr = new ThreadLocal<>();
    protected final ThreadLocal<Map<String, String>> currentObjRefs = new ThreadLocal<>();
    protected final ThreadLocal<Map<String, String>> currentVariables = new ThreadLocal<>();
    protected final Controller controller;
    protected AccessContext apiCtx;

    protected AbsApiCallHandler(Controller controllerRef, AccessContext apiCtxRef)
    {
        controller = controllerRef;
        apiCtx = apiCtxRef;
    }

    protected AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc
    )
    {
        currentAccCtx.set(accCtx);
        currentPeer.set(peer);
        currentApiCallType.set(type);
        currentApiCallRc.set(apiCallRc);
        currentObjRefs.set(new TreeMap<String, String>());
        currentVariables.set(new TreeMap<String, String>());
        return this;
    }

    @Override
    public void close() throws Exception
    {
        currentAccCtx.set(null);
        currentPeer.set(null);
        currentApiCallType.set(null);
        currentApiCallRc.set(null);
    }

    protected final NodeName asNodeName(String nodeNameStr, long retCode) throws ApiCallHandlerFailedException
    {
        try
        {
            return new NodeName(nodeNameStr);
        }
        catch (InvalidNameException e)
        {
            report(
                e,
                "The given node name '%s' is invalid.",
                retCode
            );
            throw new ApiCallHandlerFailedException();
        }
    }

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

    protected final void report (
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

    protected void addAnswer(String msg, long retCode)
    {
        addAnswer(msg, null, null, null, retCode);
    }

    protected void addAnswer(
        String msg,
        String cause,
        String details,
        String correction,
        long retCode
    )
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(retCode);
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

    protected final void handleAccDeniedExc(Exception exc, String action, long retCode)
    {
        AccessContext accCtx = currentAccCtx.get();
        report(
            exc,
            String.format("Identity '%s' using role: '%s' is not authorized to %s.",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            ),
            retCode
        );
    }

    protected final void handleSqlExc(SQLException sqlExc, String action, long retCode)
    {
        report(
            sqlExc,
            String.format(
                "A database error occured while %s.",
                action
            ),
            retCode
        );
    }

    protected static class ApiCallHandlerFailedException extends RuntimeException
    {
        private static final long serialVersionUID = -3941266567336938181L;
    }
}
