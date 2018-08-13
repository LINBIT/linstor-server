package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

public class ProtoStorPoolFreeSpaceUtils
{
    public static List<StorPoolFreeSpace> getAllStorPoolFreeSpaces(Map<StorPool, SpaceInfo> freeSpaceMap)
    {
        List<StorPoolFreeSpace> list = new ArrayList<>();

        for (Entry<StorPool, SpaceInfo> entry : freeSpaceMap.entrySet())
        {
            StorPool storPool = entry.getKey();
            list.add(
                StorPoolFreeSpace.newBuilder()
                .setStorPoolUuid(storPool.getUuid().toString())
                .setStorPoolName(storPool.getName().displayValue)
                .setFreeCapacity(entry.getValue().freeCapacity)
                .setTotalCapacity(entry.getValue().totalCapacity)
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
                    protoFreeSpace.getFreeCapacity()
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
