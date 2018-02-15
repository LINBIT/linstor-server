package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgDelRscOuterClass;

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

    @Inject
    public NotifyResourceDeleted(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelRscOuterClass.MsgDelRsc msgDeleteRsc = MsgDelRscOuterClass.MsgDelRsc.parseDelimitedFrom(msgDataIn);
        apiCallHandler.resourceDeleted(
            msgDeleteRsc.getNodeName(),
            msgDeleteRsc.getRscName()
        );
    }
}
