package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public class ProtoStorPoolFreeSpaceUtils
{
    public static List<StorPoolFreeSpace> getAllStorPoolFreeSpaces(
        Map<StorPoolInfo, SpaceInfo> freeSpaceMap
    )
    {
        List<StorPoolFreeSpace> list = new ArrayList<>();

        for (Entry<StorPoolInfo, SpaceInfo> entry : freeSpaceMap.entrySet())
        {
            StorPoolInfo storPoolInfo = entry.getKey();
            list.add(
                StorPoolFreeSpace.newBuilder()
                    .setStorPoolUuid(storPoolInfo.getUuid().toString())
                    .setStorPoolName(storPoolInfo.getName().displayValue)
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
