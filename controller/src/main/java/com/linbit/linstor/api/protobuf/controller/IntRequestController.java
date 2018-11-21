package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_CONTROLLER,
    description = "Called by the satellite to request controller update data",
    transactional = false
)
public class IntRequestController implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntRequestController(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId objId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID nodeUuid = UUID.fromString(objId.getUuid());
        String nodeName = objId.getName();

        apiCallHandler.handleControllerRequest(nodeUuid, nodeName);
    }

}
