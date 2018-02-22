package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass.MsgIntDelVlm;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_VLM_DEL,
    description = "Called by the satellite to notify the controller of successful volume deletion"
)
public class NotifyVolumeDeleted implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public NotifyVolumeDeleted(
        CtrlApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntDelVlm msgDelVlm = MsgIntDelVlm.parseDelimitedFrom(msgDataIn);
        apiCallHandler.volumeDeleted(
            msgDelVlm.getNodeName(),
            msgDelVlm.getRscName(),
            msgDelVlm.getVlmNr()
        );
    }
}
