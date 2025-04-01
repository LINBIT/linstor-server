package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.StringUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.StringJoiner;

@Singleton
public class ResponseConverter
{
    private final ErrorReporter errorReporter;

    @Inject
    public ResponseConverter(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    /**
     * Apply context including operation mask and detail to sourceResponses and add to targetResponses.
     */
    public void addWithDetail(ApiCallRcImpl targetResponses, ResponseContext context, ApiCallRc sourceResponses)
    {
        targetResponses.addEntries(addContextAll(sourceResponses, context, true));
    }

    /**
     * Apply context including operation mask and detail to sourceResponses and add to targetResponses.
     */
    public void addWithDetail(ApiCallRcImpl targetResponses, ResponseContext context, ApiCallRc.RcEntry sourceEntry)
    {
        targetResponses.addEntry(addContext(sourceEntry, context, true));
    }

    /**
     * Apply context operation mask to sourceResponses and add to targetResponses.
     */
    public void addWithOp(ApiCallRcImpl targetResponses, ResponseContext context, ApiCallRc.RcEntry entry)
    {
        targetResponses.addEntry(addContext(entry, context, false));
    }

    /**
     * Apply context to exc, report the error, and return responses for the client.
     */
    public ApiCallRcImpl reportException(Peer peer, ResponseContext context, Throwable exc)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        for (ApiCallRc.RcEntry entry : exceptionToResponse(peer, context, exc))
        {
            ApiCallRcImpl.EntryBuilder builder = ApiCallRcImpl.entryBuilder(entry, null, null);
            if (entry.skipErrorReport())
            {
                if (entry.isError())
                {
                    errorReporter.logError(entry.getMessage());
                }
                else
                {
                    errorReporter.logWarning(entry.getMessage());
                }
            }
            else
            {
                final String errorId = errorReporter.reportError(
                    exc instanceof ApiException && exc.getCause() != null ? exc.getCause() : exc,
                    peer.getAccessContext(),
                    peer,
                    entry.getMessage());
                builder.addErrorId(errorId);
            }
            apiCallRc.addEntry(builder.build());
        }

        return apiCallRc;
    }

    public Flux<ApiCallRc> reportingExceptions(ResponseContext context, Publisher<ApiCallRc> responses)
    {
        return Mono.deferContextual(Mono::just)
            .flatMapMany(subscriberContext ->
                Flux.from(responses)
                    .map(apiCallRc -> addContextAll(apiCallRc, context, false))
                    .onErrorResume(exc -> Flux.just(reportException(subscriberContext.get(Peer.class), context, exc)))
            );
    }

    public ApiCallRc exceptionToResponse(Peer peer, ResponseContext context, Throwable exc)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        if (exc instanceof ApiRcException)
        {
            ApiRcException apiRcException = (ApiRcException) exc;

            if (apiRcException.hasContext())
            {
                apiCallRc.addEntries(apiRcException.getApiCallRc());
            }
            else
            {
                ApiCallRc sourceApiCallRc = apiRcException.getApiCallRc();

                for (ApiCallRc.RcEntry sourceEntry : sourceApiCallRc)
                {
                    StringJoiner causeJoiner = new StringJoiner("\n");

                    String sourceCause = sourceEntry.getCause();
                    if (sourceCause != null)
                    {
                        causeJoiner.add(sourceCause);
                    }

                    Throwable excCause = exc.getCause();
                    if (excCause != null)
                    {
                        causeJoiner.add(excCause.getMessage());
                    }

                    apiCallRc.addEntry(ApiCallRcImpl
                        .entryBuilder(sourceEntry, null, null)
                        .setCause(causeJoiner.toString())
                        .setSkipErrorReport(sourceEntry.skipErrorReport())
                        .build()
                    );
                }
            }
        }
        else
        if (exc instanceof ApiAccessDeniedException)
        {
            ApiAccessDeniedException acExc = (ApiAccessDeniedException) exc;
            apiCallRc.addEntry(ApiCallRcImpl
                .entryBuilder(
                    acExc.getRetCode(),
                    ResponseUtils.getAccDeniedMsg(peer.getAccessContext(), acExc.getAction())
                )
                .setCause(acExc.getCause().getMessage())
                .build()
            );
        }
        else
        if (exc instanceof ApiTransactionException)
        {
            ApiTransactionException sqlExc = (ApiTransactionException) exc;
            apiCallRc.addEntry(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_SQL,
                    ResponseUtils.getSqlMsg(
                        context.getOperationDescription().getProgressive() + " " + context.getObjectDescriptionInline())
                )
                .setCause(sqlExc.getCause().getMessage())
                .build()
            );
        }
        else
        {
            String errorType;
            long retCode;
            if (exc instanceof ImplementationError)
            {
                errorType = "implementation error";
                retCode = ApiConsts.FAIL_IMPL_ERROR;
            }
            else
            {
                errorType = "unhandled exception of type " + exc.getClass().getSimpleName();
                retCode = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(ApiCallRcImpl
                .entryBuilder(
                    retCode,
                    StringUtils.firstLetterCaps(context.getOperationDescription().getNoun()) + " of " +
                        context.getObjectDescriptionInline() + " failed due to an " + errorType + "."
                )
                .build()
            );
        }

        return addContextAll(apiCallRc, context, true);
    }

    public ApiCallRc.RcEntry addContext(ApiCallRc.RcEntry sourceEntry, ResponseContext context, boolean appendDetail)
    {
        StringJoiner detailsJoiner = new StringJoiner("\n");

        if (sourceEntry.getDetails() != null)
        {
            detailsJoiner.add(sourceEntry.getDetails());
        }
        if (appendDetail && sourceEntry.appendObjectDescrptionToDetails())
        {
            detailsJoiner.add(context.getObjectDescription());
        }

        long retCode = sourceEntry.getReturnCode();
        // do not override already set masks
        if ((retCode & ApiConsts.MASK_BITS_OP) == 0)
        {
            retCode |= context.getOpMask();
        }
        if ((retCode & ApiConsts.MASK_BITS_OBJ) == 0)
        {
            retCode |= context.getObjMask();
        }

        return ApiCallRcImpl
            .entryBuilder(
                sourceEntry,
                retCode,
                null
            )
            .setDetails(detailsJoiner.toString())
            .putAllObjRefs(context.getObjRefs())
            .build();
    }

    public ApiCallRcImpl addContextAll(ApiCallRc responses, ResponseContext context, boolean appendDetail)
    {
        ApiCallRcImpl contextualResponses = new ApiCallRcImpl();
        for (ApiCallRc.RcEntry entry : responses)
        {
            contextualResponses.addEntry(addContext(entry, context, appendDetail));
        }
        return contextualResponses;
    }
}
