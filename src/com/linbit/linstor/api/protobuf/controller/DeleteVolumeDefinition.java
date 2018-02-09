package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelVlmDfnOuterClass.MsgDelVlmDfn;
import com.linbit.linstor.security.AccessContext;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class DeleteVolumeDefinition  extends BaseProtoApiCall
{
    private Controller controller;

    public DeleteVolumeDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_VLM_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a volume definition";
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
        MsgDelVlmDfn msgDelVlmDfn = MsgDelVlmDfn.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteVolumeDefinition(
                accCtx,
                client,
                msgDelVlmDfn.getRscName(),
                msgDelVlmDfn.getVlmNr()
        );

        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
