package com.linbit.utils;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;

public class UuidUtils
{
    public static final Pattern UUID_PATTERN = Pattern.compile(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    );

    private static final int UUID_BYTES = 16;

    public static byte[] asByteArray(UUID id)
    {
        ByteBuffer buffer = ByteBuffer.allocate(UUID_BYTES);
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

    public static String dbgInstanceIdString(DbgInstanceUuid objRef)
    {
        String idText;
        if (objRef != null)
        {
            UUID id = objRef.debugGetVolatileUuid();
            if (id != null)
            {
                idText = id.toString().toUpperCase();
            }
            else
            {
                idText = "<null UUID>";
            }
        }
        else
        {
            idText = "<null objRef>";
        }
        return idText;
    }

    public static boolean isUuid(String str)
    {
        return UUID_PATTERN.matcher(str).find();
    }

    public static @Nullable UUID asUuidOrNull(String str)
    {
        return isUuid(str) ? UUID.fromString(str) : null;
    }

    private UuidUtils()
    {
    }
}
