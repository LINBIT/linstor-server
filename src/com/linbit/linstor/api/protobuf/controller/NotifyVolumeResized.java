package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntResizedVlmOuterClass.MsgIntResizedVlm;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_VLM_RESIZED,
    description = "Called by the satellite to notify the controller of successful volume resize"
)
public class NotifyVolumeResized implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public NotifyVolumeResized(
        CtrlApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntResizedVlm msgResizedVlm = MsgIntResizedVlm.parseDelimitedFrom(msgDataIn);
        apiCallHandler.volumeResized(
            msgResizedVlm.getNodeName(),
            msgResizedVlm.getRscName(),
            msgResizedVlm.getVlmNr(),
            msgResizedVlm.getVlmSize()
        );
    }
}
