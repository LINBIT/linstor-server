package com.linbit.linstor.core;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.stream.Collectors;

@Singleton
public class BackgroundRunner
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;

    @Inject
    public BackgroundRunner(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
    }

    /**
     * Detach the given flux so that it runs in the background after the current API has terminated.
     */
    public void runInBackground(String operationDescription, Flux<ApiCallRc> backgroundFlux)
    {
        backgroundFlux
            .doOnSubscribe(ignored ->
                errorReporter.logDebug("Background operation " + operationDescription + " start"))
            .doOnTerminate(() ->
                errorReporter.logDebug("Background operation " + operationDescription + " end"))
            .subscriberContext(Context.of(
                ApiModule.API_CALL_NAME, operationDescription,
                AccessContext.class, apiCtx
            ))
            .subscribe(
                responses -> reportErrorResponses(operationDescription, responses),
                exc -> errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Error in background operation " + operationDescription
                )
            );
    }

    private void reportErrorResponses(String operationDescription, ApiCallRc responses)
    {
        if (ApiRcUtils.isError(responses))
        {
            errorReporter.logError(
                "Error response from background operation %s: %s",
                operationDescription,
                responses.getEntries().stream()
                    .map(ApiCallRc.RcEntry::getMessage)
                    .collect(Collectors.joining("; "))
            );
        }
    }
}
