package com.linbit.linstor.api.protobuf.controller;

import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Peer;

@ProtobufApiCall
public class RequestResource extends RequestObject
{
    public RequestResource(Controller controller)
    {
        super(
            controller,
            InternalApiConsts.API_REQUEST_RSC,
            "resource"
        );
    }

    @Override
    protected void handleRequest(String name, UUID objUuid, int msgId, Peer satellitePeer)
    {
        controller.getApiCallHandler().requestResource(satellitePeer, msgId, name, objUuid);
    }

}
