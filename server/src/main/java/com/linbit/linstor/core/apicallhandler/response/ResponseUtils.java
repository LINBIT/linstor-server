package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.util.Map;

public class ResponseUtils
{

    private ResponseUtils()
    {
    }

    /**
     * Reports the given {@link Throwable} to controller's {@link ErrorReporter} and the given
     * {@link ApiCallRcImpl}.
     * Cause, details and correction messages are left empty.
     * @param accCtx,
     * @param errorMsg
     * @param retCode
     * @param errorReporter
     * @param peer
     */
    public static void reportStatic(
        @Nullable Throwable throwableRef,
        String errorMsg,
        long retCode,
        @Nullable Map<String, String> objRefsRef,
        boolean skipErrorReport,
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
            skipErrorReport,
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
     *
     * @param errorMsg
     * @param causeMsg
     * @param detailsMsg
     * @param correctionMsg
     * @param retCode
     * @param objRefsRef
     * @param apiCallRcRef
     * @param skipErrorReport
     * @param accCtx
     * @param peer
     */
    public static void reportStatic(
        @Nullable Throwable throwableRef,
        String errorMsg,
        @Nullable String causeMsg,
        @Nullable String detailsMsg,
        @Nullable String correctionMsg,
        long retCode,
        @Nullable Map<String, String> objRefsRef,
        boolean skipErrorReport,
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
        @Nullable String errorId = null;
        if (!skipErrorReport)
        {
            errorId = errorReporter.reportError(
                throwable,
                accCtx,
                peer,
                errorMsg
            );
        }
        addAnswerStatic(
            errorMsg,
            causeMsg,
            detailsMsg,
            correctionMsg,
            retCode,
            objRefsRef,
            errorId,
            skipErrorReport,
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
     *  @param msg
     * @param cause
     * @param details
     * @param correction
     * @param retCode
     * @param objRefsRef
     * @param errorId
     * @param skipErrorReport Do not create an error-report
     */
    public static void addAnswerStatic(
        String msg,
        @Nullable String cause,
        @Nullable String details,
        @Nullable String correction,
        long retCode,
        @Nullable Map<String, String> objRefsRef,
        @Nullable String errorId,
        boolean skipErrorReport,
        ApiCallRcImpl apiCallRcRef
    )
    {

        ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
        entry.setReturnCodeBit(retCode);
        entry.setMessage(msg);
        entry.setCause(cause);
        entry.setDetails(details);
        entry.setCorrection(correction);
        entry.setSkipErrorReport(skipErrorReport);

        if (objRefsRef != null)
        {
            entry.putAllObjRef(objRefsRef);
        }

        if (errorId != null)
        {
            entry.addErrorId(errorId);
        }

        apiCallRcRef.addEntry(entry);
    }

    public static void reportSuccessStatic(
        String msg,
        @Nullable String details,
        long retCode,
        @Nullable ApiCallRcImpl apiCallRcRef,
        @Nullable Map<String, String> objsRef,
        @Nullable ErrorReporter errorReporter
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
                null, // errorId
                true,
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
            "A database error occurred while %s.",
            action
        );
    }

    public static ApiCallRc.RcEntry makeFullSyncFailedResponse(Peer satellite)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_STLT_NOT_UPDATED,
                "Satellite reported an error during fullSync. This change will NOT be " +
                    "delivered to satellte '" + satellite.getNode().getName().displayValue +
                    "' until the error is resolved. Reconnect the satellite to the controller " +
                    "to remove this blockade."
            )
            .build();
    }

    public static ApiCallRc.RcEntry makeNotConnectedWarning(NodeName nodeName)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_NOT_CONNECTED,
                "No active connection to satellite '" + nodeName + "'"
            )
            .setDetails(
                "The controller is trying to (re-) establish a connection to the satellite. " +
                    "The controller stored the changes and as soon the satellite is connected, it will " +
                    "receive this update."
            )
            .build();
    }
}
