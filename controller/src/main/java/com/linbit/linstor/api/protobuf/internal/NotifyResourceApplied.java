package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscInternalCallHandler;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass.Props;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass.SnapVlmProps;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_RSC_APPLIED,
    description = "Called by the satellite to notify the controller of successful " +
                  "resource creation, modification or deletion"
)
@Singleton
public class NotifyResourceApplied implements ApiCall
{
    private final RscInternalCallHandler rscInternalCallHandler;

    @Inject
    public NotifyResourceApplied(
        RscInternalCallHandler apiCallHandlerRef
    )
    {
        rscInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyRscSuccess msgIntAppliedRsc = MsgIntApplyRscSuccess.parseDelimitedFrom(msgDataIn);
        // TODO: Maybe introduce some tracking updateId which should help determining if this
        // applied update was the last change the controller sent to the satellite.
        // If so, we could display to the client if a resource-adjustment is pending or if the
        // satellite is basically idle

        Map<Integer, Props> vlmPropsMap = msgIntAppliedRsc.getVlmPropsMap();


        Map<Integer, Map<String, String>> vlmProps = new HashMap<>();
        for (Entry<Integer, Props> entry : vlmPropsMap.entrySet())
        {
            vlmProps.put(entry.getKey(), entry.getValue().getPropMap());
        }

        Map<String, Map<String, String>> snapProps = new HashMap<>();
        for (Entry<String, Props> entry : msgIntAppliedRsc.getSnapPropsMap().entrySet())
        {
            snapProps.put(entry.getKey(), entry.getValue().getPropMap());
        }

        Map<String, Map<Integer, Map<String, String>>> snapVlmProps = new HashMap<>();
        for (Entry<String, SnapVlmProps> entry : msgIntAppliedRsc.getSnapVlmPropsMap().entrySet())
        {
            Map<Integer, Map<String, String>> curSnapVlmProps = new HashMap<>();
            for (Entry<Integer, Props> vlmEntry : entry.getValue().getSnapVlmPropsMap().entrySet())
            {
                curSnapVlmProps.put(vlmEntry.getKey(), vlmEntry.getValue().getPropMap());
            }
            snapVlmProps.put(entry.getKey(), curSnapVlmProps);
        }

        Map<String, RscLayerDataApi> snapLayers = new HashMap<>();
        for (Entry<String, RscLayerData> entry : msgIntAppliedRsc.getSnapStorageLayerObjectsMap().entrySet())
        {
            snapLayers.put(entry.getKey(), ProtoLayerUtils.extractRscLayerData(entry.getValue(), -1, -1));
        }

        rscInternalCallHandler.updateVolume(
            msgIntAppliedRsc.getRscId().getName(),
            ProtoLayerUtils.extractRscLayerData(
                msgIntAppliedRsc.getLayerObject(),
                -1, // we are on the controller now, so we do not care about fullSyncdId
                -1  // we are on the controller now, so we do not care about updateId
            ),
            msgIntAppliedRsc.getRscPropsMap(),
            vlmProps,
            snapProps,
            snapVlmProps,
            snapLayers
        );
    }
}
