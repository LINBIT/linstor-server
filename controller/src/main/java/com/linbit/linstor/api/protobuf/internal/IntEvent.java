package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.event.EventProcessor;
import com.linbit.linstor.proto.responses.MsgEventOuterClass;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = ApiConsts.API_EVENT,
    description = "Handles an event",
    transactional = true
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

        // intentionally not calling eventProcessor.handleEvent directly to prevent the TCP thread from blocking
        return Flux.defer(
            // locks are taken within the handleEvent method
            () -> eventProcessor.handleEvent(
                    msgEvent.getEventAction(),
                    msgEvent.getEventName(),
                    msgEvent.hasResourceName() ? msgEvent.getResourceName() : null,
                    msgEvent.hasVolumeNumber() ? msgEvent.getVolumeNumber() : null,
                    msgEvent.hasSnapshotName() ? msgEvent.getSnapshotName() : null,
                    msgEvent.hasPeerName() ? msgEvent.getPeerName() : null,
                    msgDataIn
                )
            )
            .thenMany(Flux.empty());
    }
}
