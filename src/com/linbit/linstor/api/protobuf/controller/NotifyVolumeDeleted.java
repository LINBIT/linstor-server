package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntDelVlmOuterClass;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class NotifyVolumeDeleted extends BaseProtoApiCall {
    private final Controller controller;

    public NotifyVolumeDeleted(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_NOTIFY_VLM_DEL;
    }

    @Override
    public String getDescription()
    {
        return "Called by the satellite to notify the controller of successful volume deletion";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgIntDelVlmOuterClass.MsgIntDelVlm msgDelVlm = MsgIntDelVlmOuterClass.MsgIntDelVlm.parseDelimitedFrom(msgDataIn);
        controller.getApiCallHandler().volumeDeleted(
            accCtx,
            client,
            msgDelVlm.getNodeName(),
            msgDelVlm.getRscName(),
            msgDelVlm.getVlmNr()
        );
    }
}
