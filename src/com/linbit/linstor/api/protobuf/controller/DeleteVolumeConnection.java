package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelVlmConnOuterClass.MsgDelVlmConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class DeleteVolumeConnection extends BaseProtoApiCall
{
    private final Controller controller;

    public DeleteVolumeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_VLM_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes volume connection options";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgDelVlmConn msgDeleteVlmConn = MsgDelVlmConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteVolumeConnection(
            accCtx,
            client,
            msgDeleteVlmConn.getNodeName1(),
            msgDeleteVlmConn.getNodeName2(),
            msgDeleteVlmConn.getResourceName(),
            msgDeleteVlmConn.getVolumeNr()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
