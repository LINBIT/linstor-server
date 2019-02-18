package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.VlmUpdatePojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtoRscLayerUtils;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscInternalCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

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

        rscInternalCallHandler.updateVolumeData(
            msgIntAppliedRsc.getRscId().getName(),
            ProtoRscLayerUtils.extractLayerData(msgIntAppliedRsc.getLayerObject()),
            ProtoStorPoolFreeSpaceUtils.toFreeSpacePojo(
                msgIntAppliedRsc.getFreeSpaceList()
            )
        );
    }
}
