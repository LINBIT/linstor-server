package com.linbit.linstor.api.interfaces;

public interface Serializer<TYPE>
{
    public byte[] getChangedMessage(TYPE data);

    public byte[] getDataMessage(int msgId, TYPE data);
}
