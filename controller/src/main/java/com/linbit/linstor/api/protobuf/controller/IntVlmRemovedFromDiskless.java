package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.proto.javainternal.MsgIntVlmRemovedFromDisklessOuterClass.MsgIntVlmRemovedFromDiskless;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_VLM_REMOVED_FROM_DISKLESS,
    description = "Called by the satellite that a volume was removed from its diskless storPool"
)
public class IntVlmRemovedFromDiskless implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntVlmRemovedFromDiskless(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntVlmRemovedFromDiskless msg = MsgIntVlmRemovedFromDiskless.parseDelimitedFrom(msgDataIn);
        apiCallHandler.vlmRemovedFromDiskless(
            UUID.fromString(msg.getVlmUuid()),
            msg.getNodeName(),
            msg.getRscName(),
            msg.getVlmNr(),
            UUID.fromString(msg.getStorPoolUuid()),
            msg.getStorPoolName(),
            msg.getFreeSpace()
        );
    }
}
