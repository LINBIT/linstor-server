package com.linbit.linstor.api.protobuf.common;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall(
    name = "Ping",
    description = "Ping: Communication test. Responds with a Pong message."
)
@Singleton
public class Ping implements ApiCallReactive
{
    private final CommonSerializer commonSerializer;

    @Inject
    public Ping(CommonSerializer commonSerializerRef)
    {
        commonSerializer = commonSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        return Mono.deferContextual(Mono::just)
            .flatMapMany(subscriberContext ->
                Flux.just(commonSerializer.answerBuilder(
                    "Pong",
                    subscriberContext.get(ApiModule.API_CALL_ID)
                ).build())
            );
    }
}
