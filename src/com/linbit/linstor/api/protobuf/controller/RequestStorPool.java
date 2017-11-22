package com.linbit.linstor.api.protobuf.controller;

import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Peer;

@ProtobufApiCall
public class RequestStorPool extends RequestObject
{
    public RequestStorPool(Controller controller)
    {
        super(
            controller,
            InternalApiConsts.API_REQUEST_STOR_POOL,
            "storpool"
        );
    }

    @Override
    protected void handleRequest(String name, UUID objUuid, int msgId, Peer satellitePeer)
    {
        controller.getApiCallHandler().requestStorPool(satellitePeer, msgId, name, objUuid);
    }
}
