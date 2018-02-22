package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.core.StltApiCallHandlerUtils;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.storage.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

public class ProtoStorPoolFreeSpaceUtils
{
    public static Iterable<StorPoolFreeSpace> getAllStorPoolFreeSpaces(
        StltApiCallHandlerUtils apiCallHandlerUtils
    )
        throws StorageException
    {

        Map<StorPool, Long> freeSpaceMap = apiCallHandlerUtils.getFreeSpace();
        return getAllStorPoolFreeSpaces(freeSpaceMap);
    }

    public static List<StorPoolFreeSpace> getAllStorPoolFreeSpaces(Map<StorPool, Long> freeSpaceMap)
    {
        List<StorPoolFreeSpace> list = new ArrayList<>();

        for (Entry<StorPool, Long> entry : freeSpaceMap.entrySet())
        {
            StorPool storPool = entry.getKey();
            list.add(
                StorPoolFreeSpace.newBuilder()
                .setStorPoolUuid(storPool.getUuid().toString())
                .setStorPoolName(storPool.getName().displayValue)
                .setFreeSpace(entry.getValue())
                .build()
                );
        }
        return list;
    }

    public static FreeSpacePojo[] toFreeSpacePojo(List<StorPoolFreeSpace> protoList)
    {
        List<FreeSpacePojo> list = new ArrayList<>();
        for (StorPoolFreeSpace protoFreeSpace : protoList)
        {
            list.add(
                new FreeSpacePojo(
                    UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                    protoFreeSpace.getStorPoolName(),
                    protoFreeSpace.getFreeSpace()
                )
            );
        }

        FreeSpacePojo[] fspArr = new FreeSpacePojo[list.size()];
        list.toArray(fspArr);
        return fspArr;
    }

    private ProtoStorPoolFreeSpaceUtils()
    {
    }
}
