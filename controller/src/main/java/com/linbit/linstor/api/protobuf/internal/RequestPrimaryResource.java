package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscDfnInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntPrimaryOuterClass.MsgIntPrimary;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_PRIMARY_RSC,
    description = "Satellite request primary for a resource"
)
@Singleton
public class RequestPrimaryResource implements ApiCall
{
    private final RscDfnInternalCallHandler rscDfnInternalCallHandler;

    @Inject
    public RequestPrimaryResource(RscDfnInternalCallHandler apiCallHandlerRef)
    {
        rscDfnInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntPrimary msgReqPrimary = MsgIntPrimary.parseDelimitedFrom(msgDataIn);
        rscDfnInternalCallHandler.handlePrimaryResourceRequest(
            msgReqPrimary.getRscName(),
            ProtoUuidUtils.deserialize(msgReqPrimary.getRscUuid()),
            msgReqPrimary.getAlreadyInitialized()
        );
    }
}
