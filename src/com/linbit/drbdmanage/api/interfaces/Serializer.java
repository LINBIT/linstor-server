package com.linbit.drbdmanage.api.interfaces;

public interface Serializer<TYPE>
{
    public byte[] getChangedMessage(TYPE data);

    public byte[] getDataMessage(int msgId, TYPE data);
}
