package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.VlmUpdatePojo;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_RSC_APPLIED,
    description = "Called by the satellite to notify the controller of successful " +
                  "resource creation or modification"
)
public class NotifyResourceApplied implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer satellitePeer;

    @Inject
    public NotifyResourceApplied(
        CtrlApiCallHandler apiCallHandlerRef,
        Peer satellitePeerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        satellitePeer = satellitePeerRef;
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

        apiCallHandler.updateVolumeData(
            satellitePeer,
            msgIntAppliedRsc.getRscId().getName(),
            msgIntAppliedRsc.getVlmDataList().stream()
                .map(v -> new VlmUpdatePojo(v.getVlmNr(), v.getBlockDevicePath(), v.getMetaDisk()))
                .collect(Collectors.toList())
        );

        apiCallHandler.updateRealFreeSpace(
            satellitePeer,
            ProtoStorPoolFreeSpaceUtils.toFreeSpacePojo(
                msgIntAppliedRsc.getFreeSpaceList()
            )
        );
    }
}
