package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.CommonSerializerBuilderImpl;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgEventOuterClass;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.VlmStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Singleton
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
    public CommonSerializerBuilder builder()
    {
        return builder(null);
    }

    @Override
    public CommonSerializerBuilder builder(String apiCall)
    {
        return builder(apiCall, null);
    }

    @Override
    public CommonSerializerBuilder builder(String apiCall, Integer msgId)
    {
        return new CommonSerializerBuilderImpl(errorReporter, this, apiCall, msgId);
    }

    @Override
    public void writeHeader(String apiCall, Integer msgId, ByteArrayOutputStream baos) throws IOException
    {
        MsgHeaderOuterClass.MsgHeader.Builder builder = MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(apiCall);
        if (msgId != null)
        {
            builder
                .setMsgId(msgId);
        }
        builder
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeEvent(
        Integer watchId, EventIdentifier eventIdentifier, ByteArrayOutputStream baos
    )
        throws IOException
    {
        MsgEventOuterClass.MsgEvent.Builder eventBuilder = MsgEventOuterClass.MsgEvent.newBuilder();

        if (watchId != null)
        {
            eventBuilder.setWatchId(watchId);
        }

        eventBuilder.setEventName(eventIdentifier.getEventName());

        if (eventIdentifier.getResourceName() != null)
        {
            eventBuilder.setResourceName(eventIdentifier.getResourceName().displayValue);
        }

        if (eventIdentifier.getNodeName() != null)
        {
            eventBuilder.setNodeName(eventIdentifier.getNodeName().displayValue);
        }

        if (eventIdentifier.getVolumeNumber() != null)
        {
            eventBuilder.setVolumeNumber(eventIdentifier.getVolumeNumber().value);
        }

        eventBuilder.build().writeDelimitedTo(baos);
    }

    @Override
    public void writeVolumeDiskState(String diskState, ByteArrayOutputStream baos)
        throws IOException
    {
        EventVlmDiskStateOuterClass.EventVlmDiskState.newBuilder()
            .setDiskState(diskState)
            .build()
            .writeDelimitedTo(baos);
    }

    @Override
    public void writeResourceStateEvent(String resourceStateString, ByteArrayOutputStream baos)
        throws IOException
    {
        EventRscStateOuterClass.EventRscState.newBuilder()
            .setState(resourceStateString)
            .build()
            .writeDelimitedTo(baos);
    }

    private static VlmStateOuterClass.VlmState buildVolumeState(VolumeState vlmState)
    {
        VlmStateOuterClass.VlmState.Builder vlmStateBuilder = VlmStateOuterClass.VlmState.newBuilder();

        vlmStateBuilder.setVlmNr(vlmState.getVlmNr().value);

        if (vlmState.isPresent() != null)
        {
            vlmStateBuilder.setIsPresent(vlmState.isPresent());
        }

        if (vlmState.hasDisk() != null)
        {
            vlmStateBuilder.setHasDisk(vlmState.hasDisk());
        }

        if (vlmState.hasMetaData() != null)
        {
            vlmStateBuilder.setHasMetaData(vlmState.hasMetaData());
        }

        if (vlmState.isCheckMetaData() != null)
        {
            vlmStateBuilder.setCheckMetaData(vlmState.isCheckMetaData());
        }

        if (vlmState.isDiskFailed() != null)
        {
            vlmStateBuilder.setDiskFailed(vlmState.isDiskFailed());
        }

        if (vlmState.getNetSize() != null)
        {
            vlmStateBuilder.setNetSize(vlmState.getNetSize());
        }

        if (vlmState.getGrossSize() != null)
        {
            vlmStateBuilder.setGrossSize(vlmState.getGrossSize());
        }

        if (vlmState.getMinorNr() != null)
        {
            vlmStateBuilder.setVlmMinorNr(vlmState.getMinorNr().value);
        }

        if (vlmState.getDiskState() != null)
        {
            vlmStateBuilder.setDiskState(vlmState.getDiskState());
        }

        return vlmStateBuilder.build();
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
            rscStateBuilder.addVlmStates(buildVolumeState(vlmState));
        }

        return rscStateBuilder.build();
    }
}
