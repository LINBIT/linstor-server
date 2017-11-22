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
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

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
        return InternalApiConsts.API_CHANGED_RSC;
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
        // but for testing purposes, we immediately ask the controller for the update

        try
        {
            Message message = controllerPeer.createMessage();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            String rscName = rscId.getName();
            UUID rscUuid = asUuid(rscId.getUuid());

            // TODO: remember to request this resource later.

            // FIXME: remove this testblock and replace it with an async request
            {
                // do this when satellite has finished its current task
                MsgHeader.newBuilder()
                    .setApiCall(InternalApiConsts.API_REQUEST_RSC)
                    .setMsgId(msgId)
                    .build()
                    .writeDelimitedTo(baos);

                rscId.writeDelimitedTo(baos); // only as a workaround

                byte[] data = baos.toByteArray();
                message.setData(data);
                controllerPeer.sendMessage(message);
            }
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
