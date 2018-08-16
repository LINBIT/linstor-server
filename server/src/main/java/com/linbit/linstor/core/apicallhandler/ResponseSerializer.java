package com.linbit.linstor.core.apicallhandler;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ResponseSerializer
{
    private final CommonSerializer commonSerializer;

    @Inject
    public ResponseSerializer(CommonSerializer commonSerializerRef)
    {
        commonSerializer = commonSerializerRef;
    }

    public Flux<byte[]> transform(Publisher<ApiCallRc> responses)
    {
        return Mono.subscriberContext()
            .flatMapMany(subscriberContext ->
                Flux.from(responses).map(response ->
                    commonSerializer.answerBuilder(ApiConsts.API_REPLY, subscriberContext.get(ApiModule.API_CALL_ID))
                        .apiCallRcSeries(response)
                        .build())
            );
    }
}
