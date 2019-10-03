package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.util.function.Tuple2;

public class CtrlResponseUtils
{
    private CtrlResponseUtils()
    {
    }

    /**
     * Combine responses from multiple nodes.
     * <p>
     * When the response from the node is successful, it is replaced with the given message.
     * <p>
     * When failures occur they are converted into responses and a {@link CtrlResponseUtils.DelayedApiRcException}
     * is emitted after all the updates have terminated.
     */
    public static Flux<ApiCallRc> combineResponses(
        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> responses,
        ResourceName rscName,
        String messageFormat
    )
    {
        return combineResponses(responses, rscName, Collections.emptySet(), messageFormat, messageFormat);
    }

    /**
     * Like {@link #combineResponses(Flux, ResourceName, String)}, but the success message is chosen based on whether
     * the node is in the given collection.
     */
    public static Flux<ApiCallRc> combineResponses(
        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> responses,
        ResourceName rscName,
        Collection<NodeName> nodeNames,
        String messageFormatThese,
        String messageFormatOthers
    )
    {
        return responses
            .map(namedResponse ->
                {
                    NodeName nodeName = namedResponse.getT1();
                    Flux<ApiCallRc> nodeResponses = namedResponse.getT2();

                    Flux<ApiCallRc> extraResponses;
                    String messageFormat = nodeNames.contains(nodeName) ?
                        messageFormatThese : messageFormatOthers;
                    if (messageFormat != null)
                    {
                        extraResponses = Flux.just(ApiCallRcImpl.singletonApiCallRc((ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            MessageFormat.format(
                                messageFormat,
                                "'" + nodeName + "'",
                                "'" + rscName + "'"
                            )
                        ))));
                    }
                    else
                    {
                        extraResponses = Flux.empty();
                    }

                    return nodeResponses.thenMany(extraResponses);
                }
            )
            .transform(CtrlResponseUtils::mergeExtractingApiRcExceptions);
    }

    /**
     * Merge the sources, delaying failure.
     * Any {@link ApiRcException} errors are suppressed and converted into normal responses.
     * If any errors were suppressed, a token {@link DelayedApiRcException} error is emitted when all sources complete.
     */
    private static Flux<ApiCallRc> mergeExtractingApiRcExceptions(
        Publisher<? extends Publisher<ApiCallRc>> sources)
    {
        return Flux
            .merge(
                Flux.from(sources)
                    .map(source ->
                        Flux.from(source)
                            .map(Signal::next)
                            .onErrorResume(ApiRcException.class, error -> Flux.just(Signal.error(error)))
                    )
            )
            .compose(signalFlux ->
                {
                    List<ApiRcException> errors = Collections.synchronizedList(new ArrayList<>());
                    return signalFlux
                        .map(signal ->
                            {
                                ApiCallRc apiCallRc;
                                ApiRcException apiRcException = (ApiRcException) signal.getThrowable();
                                if (apiRcException != null)
                                {
                                    errors.add(apiRcException);
                                    apiCallRc = apiRcException.getApiCallRc();
                                }
                                else
                                {
                                    apiCallRc = signal.get();
                                }
                                return apiCallRc;
                            }
                        )
                        .concatWith(Flux.defer(() ->
                            errors.isEmpty() ?
                                Flux.empty() :
                                Flux.error(new DelayedApiRcException(errors))
                        ));
                }
            );
    }

    /**
     * See {@link #combineResponses(Flux, ResourceName, String)}.
     */
    public static class DelayedApiRcException extends RuntimeException
    {
        private final List<ApiRcException> errors;

        public DelayedApiRcException(List<ApiRcException> errorsRef)
        {
            super("Exceptions have been converted to responses");
            errors = errorsRef;
            for (ApiRcException exc : errorsRef)
            {
                addSuppressed(exc);
            }
        }

        public List<ApiRcException> getErrors()
        {
            return errors;
        }
    }
}
