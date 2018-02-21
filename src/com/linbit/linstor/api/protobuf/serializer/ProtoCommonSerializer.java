package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.VlmStateOuterClass;

public class ProtoCommonSerializer
{
    public static RscStateOuterClass.RscState buildResourceState(
        final String nodeName,
        final ResourceState rscState
    )
    {
        RscStateOuterClass.RscState.Builder rscStateBuilder = RscStateOuterClass.RscState.newBuilder();

        rscStateBuilder
            .setRscName(rscState.getRscName())
            .setNodeName(nodeName)
            .setIsPresent(rscState.isPresent())
            .setRequiresAdjust(rscState.requiresAdjust())
            .setIsPrimary(rscState.isPrimary());

        // volumes
        for (VolumeState vlmState : rscState.getVolumes())
        {
            VlmStateOuterClass.VlmState.Builder vlmStateBuilder = VlmStateOuterClass.VlmState.newBuilder();

            vlmStateBuilder
                .setVlmNr(vlmState.getVlmNr().value)
                .setVlmMinorNr(vlmState.getMinorNr().value)
                .setIsPresent(vlmState.isPresent())
                .setHasDisk(vlmState.hasDisk())
                .setHasMetaData(vlmState.hasMetaData())
                .setCheckMetaData(vlmState.isCheckMetaData())
                .setDiskFailed(vlmState.isDiskFailed())
                .setNetSize(vlmState.getNetSize())
                .setGrossSize(vlmState.getGrossSize());

            if (vlmState.getDiskState() != null)
            {
                vlmStateBuilder.setDiskState(vlmState.getDiskState());
            }
            rscStateBuilder.addVlmStates(vlmStateBuilder);
        }

        return rscStateBuilder.build();
    }
}
