package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.ConfInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_CONTROLLER,
    description = "Called by the satellite to request controller update data",
    transactional = false
)
@Singleton
public class IntRequestController implements ApiCall
{
    private final ConfInternalCallHandler confInternalCallHandler;

    @Inject
    public IntRequestController(ConfInternalCallHandler apiCallHandlerRef)
    {
        confInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId objId = IntObjectId.parseDelimitedFrom(msgDataIn);
        UUID nodeUuid = ProtoUuidUtils.deserialize(objId.getUuid());
        String nodeName = objId.getName();

        confInternalCallHandler.handleControllerRequest(nodeUuid, nodeName);
    }

}
