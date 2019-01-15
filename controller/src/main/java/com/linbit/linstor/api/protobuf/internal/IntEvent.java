package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.event.EventProcessor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgEventOuterClass;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_EVENT,
    description = "Handles an event"
)
@Singleton
public class IntEvent implements ApiCallReactive
{
    private final EventProcessor eventProcessor;

    @Inject
    public IntEvent(
        EventProcessor eventProcessorRef
    )
    {
        eventProcessor = eventProcessorRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgEventOuterClass.MsgEvent msgEvent = MsgEventOuterClass.MsgEvent.parseDelimitedFrom(msgDataIn);

        return Mono.subscriberContext()
            .doOnNext(subscriberContext ->
                eventProcessor.handleEvent(
                    msgEvent.getEventAction(),
                    msgEvent.getEventName(),
                    msgEvent.hasResourceName() ? msgEvent.getResourceName() : null,
                    msgEvent.hasVolumeNumber() ? msgEvent.getVolumeNumber() : null,
                    msgEvent.hasSnapshotName() ? msgEvent.getSnapshotName() : null,
                    subscriberContext.get(Peer.class),
                    msgDataIn
                )
            )
            .thenMany(Flux.empty());
    }
}
