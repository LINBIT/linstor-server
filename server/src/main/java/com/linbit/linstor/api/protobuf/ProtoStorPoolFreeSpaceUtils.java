package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;

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

    public static List<CapacityInfoPojo> toFreeSpacePojo(List<StorPoolFreeSpace> protoList)
    {
        List<CapacityInfoPojo> list = new ArrayList<>();
        for (StorPoolFreeSpace protoFreeSpace : protoList)
        {
            list.add(
                new CapacityInfoPojo(
                    UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                    protoFreeSpace.getStorPoolName(),
                    protoFreeSpace.getFreeCapacity(),
                    protoFreeSpace.getTotalCapacity(),
                    ProtoDeserializationUtils.parseApiCallRcList(protoFreeSpace.getErrorsList())
                )
            );
        }
        return list;
    }

    private ProtoStorPoolFreeSpaceUtils()
    {
    }
}
