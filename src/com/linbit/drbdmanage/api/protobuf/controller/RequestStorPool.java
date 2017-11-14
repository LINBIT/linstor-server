package com.linbit.drbdmanage.api.protobuf.controller;

import java.util.UUID;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Peer;

@ProtobufApiCall
public class RequestStorPool extends RequestObject
{
    public RequestStorPool(Controller controller)
    {
        super(
            controller,
            InternalApiConsts.API_STOR_POOL_REQ,
            "storpool"
        );
    }

    @Override
    protected void handleRequest(String name, UUID objUuid, int msgId, Peer satellitePeer)
    {
        controller.getApiCallHandler().requestStorPool(name, objUuid, msgId, satellitePeer);
    }
}
