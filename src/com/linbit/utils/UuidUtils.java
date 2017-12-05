package com.linbit.utils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidUtils
{
    public static byte[] asByteArray(UUID id)
    {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return buffer.array();
    }

    public static UUID asUuid(byte[] arr)
    {
        ByteBuffer buffer = ByteBuffer.wrap(arr);
        long mostSig = buffer.getLong();
        long leastSig = buffer.getLong();
        return new UUID(mostSig, leastSig);
    }

    private UuidUtils()
    {
    }
}
