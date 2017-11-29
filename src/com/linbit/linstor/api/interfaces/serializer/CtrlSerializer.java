package com.linbit.linstor.api.interfaces.serializer;

public interface CtrlSerializer<TYPE>
{
    public byte[] getChangedMessage(TYPE data);

    public byte[] getDataMessage(int msgId, TYPE data);
}
