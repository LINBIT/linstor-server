package com.linbit.linstor.event.serializer.protobuf.common;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.common.DonePercentageEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

@ProtobufEventSerializer(
    eventName = InternalApiConsts.EVENT_DONE_PERCENTAGE,
    objectType = WatchableObject.VOLUME
)
@Singleton
public class DonePercentageEventSerializer
    implements EventSerializer, EventSerializer.Serializer<PairNonNull<String, Optional<Float>>>
{
    private final CommonSerializer commonSerializer;
    private final DonePercentageEvent donePercentageEvent;

    @Inject
    public DonePercentageEventSerializer(
        CommonSerializer commonSerializerRef,
        DonePercentageEvent donePercentageEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        donePercentageEvent = donePercentageEventRef;
    }

    @Override
    public Serializer<PairNonNull<String, Optional<Float>>> get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(PairNonNull<String, Optional<Float>> donePercentage)
    {
        return commonSerializer.headerlessBuilder()
            .donePercentageEvent(donePercentage.objA, donePercentage.objB.orElse(null))
            .build();
    }

    @Override
    public LinstorEvent<PairNonNull<String, Optional<Float>>> getEvent()
    {
        return donePercentageEvent.get();
    }
}
