package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgRscFailedOuterClass.MsgRscFailed;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_RSC_FAILED,
    description = "Called by the satellite to notify the controller of a failed resource"
)
@Singleton
public class NotifyResourceFailed implements ApiCall
{
    private final RscInternalCallHandler rscInternalCallHandler;

    @Inject
    public NotifyResourceFailed(
        RscInternalCallHandler rscInternalCallHandlerRef
    )
    {
        rscInternalCallHandler = rscInternalCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgRscFailed msgRscFailed = MsgRscFailed.parseDelimitedFrom(msgDataIn);

        rscInternalCallHandler.handleResourceFailed(
            msgRscFailed.getRsc().getNodeName(),
            msgRscFailed.getRsc().getName()
        );
    }
}
