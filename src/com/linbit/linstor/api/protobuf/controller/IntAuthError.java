package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.security.AccessContext;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class IntAuthError extends BaseProtoApiCall
{
    private Controller controller;

    public IntAuthError(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_AUTH_ERROR;
    }

    @Override
    public String getDescription()
    {
        return "Satellite will call this api if an error happened during authentication.";
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
        client.setAuthenticated(false);
        MsgApiCallResponse msgApiCallRc = MsgApiCallResponse.parseDelimitedFrom(msgDataIn);

        errorReporter.logError("Satellite authentication error: " + msgApiCallRc.getCauseFormat());
    }

}
