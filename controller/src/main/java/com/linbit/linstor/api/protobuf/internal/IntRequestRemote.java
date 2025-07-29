package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RemoteInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_REMOTE,
    description = "Called by the satellite to request remote update data",
    transactional = false
)
@Singleton
public class IntRequestRemote implements ApiCall
{
    private final RemoteInternalCallHandler remoteInternalCallHandler;

    @Inject
    public IntRequestRemote(RemoteInternalCallHandler apiCallHandlerRef)
    {
        remoteInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId remoteId = IntObjectId.parseDelimitedFrom(msgDataIn);
        UUID remoteUuid = ProtoUuidUtils.deserialize(remoteId.getUuid());
        String remoteNameStr = remoteId.getName();

        remoteInternalCallHandler.handleRemoteRequest(remoteUuid, remoteNameStr);
    }
}
