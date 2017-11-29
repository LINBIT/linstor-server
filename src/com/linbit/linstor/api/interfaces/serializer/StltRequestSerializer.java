package com.linbit.linstor.api.interfaces.serializer;

import java.util.UUID;

import com.linbit.GenericName;

public interface StltRequestSerializer<TYPE extends GenericName>
{
    public byte[] getRequestMessage(int msgId, UUID uuid, TYPE name);
}
