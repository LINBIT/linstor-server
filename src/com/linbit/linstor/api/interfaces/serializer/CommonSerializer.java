package com.linbit.linstor.api.interfaces.serializer;

import com.linbit.linstor.event.EventIdentifier;

public interface CommonSerializer
{
    CommonSerializerBuilder builder();

    CommonSerializerBuilder builder(String apiCall);

    CommonSerializerBuilder builder(String apiCall, Integer msgId);

    public interface CommonSerializerBuilder
    {
        byte[] build();

        CommonSerializerBuilder event(Integer watchId, EventIdentifier eventIdentifier);

        CommonSerializerBuilder volumeDiskState(String diskState);

        CommonSerializerBuilder resourceStateEvent(String resourceStateString);
    }
}
