package com.linbit.linstor.api.interfaces.serializer;

public interface CommonSerializer
{
    CommonSerializerBuilder builder(String apiCall, int msgId);

    public interface CommonSerializerBuilder
    {
        byte[] build();
    }
}
