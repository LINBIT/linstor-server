package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
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

        ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
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

    public static Flux<ApiCallRc> translateDeploymentSuccess(
        Flux<Tuple2<NodeName, ApiCallRc>> responses,
        String messageFormat
    )
    {
        return translateDeploymentSuccess(responses, Collections.emptySet(), messageFormat, messageFormat);
    }

    public static Flux<ApiCallRc> translateDeploymentSuccess(
        Flux<Tuple2<NodeName, ApiCallRc>> responses,
        Collection<NodeName> nodeNames,
        String messageFormatSelf,
        String messageFormatPeer
    )
    {
        return responses.map(namedResponse ->
            {
                NodeName responseNodeName = namedResponse.getT1();
                ApiCallRc response = namedResponse.getT2();
                ApiCallRcImpl transformedResponses = new ApiCallRcImpl();
                for (ApiCallRc.RcEntry rcEntry : response.getEntries())
                {
                    if (rcEntry.getReturnCode() == ApiConsts.MODIFIED)
                    {
                        String messageFormat = nodeNames.contains(responseNodeName) ?
                            messageFormatSelf : messageFormatPeer;
                        if (messageFormat != null)
                        {
                            transformedResponses.addEntry(ApiCallRcImpl.simpleEntry(
                                ApiConsts.MODIFIED,
                                MessageFormat.format(
                                    messageFormat,
                                    "'" + responseNodeName.displayValue + "'",
                                    "'" + rcEntry.getObjRefs().get(ApiConsts.KEY_RSC_DFN) + "'"
                                )
                            ));
                        }
                    }
                    else
                    {
                        transformedResponses.addEntry(rcEntry);
                    }
                }
                return transformedResponses;
            }
        );
    }
}
