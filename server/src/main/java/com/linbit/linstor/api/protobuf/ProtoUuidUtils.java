package com.linbit.linstor.api.protobuf;


import com.linbit.linstor.proto.common.ids.UuidOuterClass;

import java.util.UUID;

public class ProtoUuidUtils
{
    private ProtoUuidUtils()
    {
    }

    public static UuidOuterClass.Uuid serialize(UUID uuidRef)
    {
        return UuidOuterClass.Uuid.newBuilder()
            .setMostSignificantBits(uuidRef.getMostSignificantBits())
            .setLeastSignificantBits(uuidRef.getLeastSignificantBits())
            .build();
    }

    public static UUID deserialize(UuidOuterClass.Uuid uuidRef)
    {
        return new UUID(uuidRef.getMostSignificantBits(), uuidRef.getLeastSignificantBits());
    }
}
