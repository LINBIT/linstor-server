package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.CommonSerializerBuilderImpl;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.VlmStateOuterClass;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProtoCommonSerializer implements CommonSerializer, CommonSerializerBuilderImpl.CommonSerializerWriter
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;

    @Inject
    public ProtoCommonSerializer(
        final ErrorReporter errReporterRef,
        final @ApiContext AccessContext serializerCtxRef
    )
    {
        this.errorReporter = errReporterRef;
        this.serializerCtx = serializerCtxRef;
    }

    @Override
    public CommonSerializerBuilder builder(String apiCall, int msgId)
    {
        return new CommonSerializerBuilderImpl(errorReporter, this, apiCall, msgId);
    }

    @Override
    public void writeHeader(String apiCall, int msgId, ByteArrayOutputStream baos) throws IOException
    {
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(apiCall)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);
    }

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
