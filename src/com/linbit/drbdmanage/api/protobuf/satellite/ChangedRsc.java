package com.linbit.drbdmanage.api.protobuf.satellite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscIdOuterClass.MsgIntRscId;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class ChangedRsc extends BaseProtoApiCall
{
    public ChangedRsc(Satellite satellite)
    {
        super(satellite.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_RSC_CHANGED;
    }

    @Override
    public String getDescription()
    {
        return "Controller calls this API when a resource has changed and this satellite should " +
            "ask for the change";
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
        // TODO: we should wait until our current task is done
        // but for testing purposes, we just as the controller for the update

        try
        {
            Message message = controllerPeer.createMessage();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            MsgIntRscId rscId = MsgIntRscId.parseDelimitedFrom(msgDataIn);
            String rscName = rscId.getResourceName();
            // TODO: UUID

            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_REQ_RSC)
                .setMsgId(msgId)
                .build().writeDelimitedTo(baos);

            rscId.writeDelimitedTo(baos); // only as a workaround

            byte[] data = baos.toByteArray();
            message.setData(data);
            controllerPeer.sendMessage(message);

        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Creating/Sending the resource-changed-query failed",
                    illegalMessageStateExc
                )
            );
        }

    }
}
