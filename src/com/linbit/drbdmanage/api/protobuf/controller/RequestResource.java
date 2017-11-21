package com.linbit.drbdmanage.api.protobuf.controller;

import java.util.UUID;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Peer;

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
