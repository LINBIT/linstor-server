package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelRscOuterClass.MsgDelRsc;
import com.linbit.linstor.proto.MsgIntDelRscOuterClass.MsgIntDelRsc;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_RSC_DEL,
    description = "Called by the satellite to notify the controller of successful resource deletion"
)
public class NotifyResourceDeleted implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer satellitePeer;

    @Inject
    public NotifyResourceDeleted(
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
        MsgIntDelRsc msgIntDeleteRsc = MsgIntDelRsc.parseDelimitedFrom(msgDataIn);
        MsgDelRsc msgDeletedRsc = msgIntDeleteRsc.getDeletedRsc();
        apiCallHandler.resourceDeleted(
            msgDeletedRsc.getNodeName(),
            msgDeletedRsc.getRscName()
        );
        apiCallHandler.updateRealFreeSpace(
            satellitePeer,
            ProtoStorPoolFreeSpaceUtils.toFreeSpacePojo(
                msgIntDeleteRsc.getFreeSpaceList()
            )
        );
    }
}
