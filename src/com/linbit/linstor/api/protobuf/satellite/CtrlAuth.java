package com.linbit.linstor.api.protobuf.satellite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class CtrlAuth extends BaseProtoApiCall
{
    private Satellite satellite;

    public CtrlAuth(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_AUTH;
    }

    @Override
    public String getDescription()
    {
        return "The authentication api the controller has to call first";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer controllerPeer
    )
        throws IOException
    {
        // TODO: implement authentication
        MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
        String nodeName = auth.getNodeName();
        UUID nodeUuid = UuidUtils.asUuid(auth.getNodeUuid().toByteArray());

        satellite.getErrorReporter().logInfo("Controller connected and authenticated");
        satellite.setControllerPeer(controllerPeer, nodeUuid, nodeName);

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_AUTH_ACCEPT)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);
            Message response = controllerPeer.createMessage();
            response.setData(baos.toByteArray());
            controllerPeer.sendMessage(response);
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            satellite.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite failed to respond controller",
                    illegalMessageStateExc
                )
            );
        }
    }
}
