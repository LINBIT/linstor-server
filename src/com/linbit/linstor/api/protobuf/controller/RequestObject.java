package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

abstract class RequestObject extends BaseProtoApiCall
{
    protected Controller controller;
    private String apiCallName;
    private String descriptionType;

    RequestObject(
        Controller controller,
        String apiCallName,
        String descriptionType
    )
    {
        super(controller.getErrorReporter());
        this.controller = controller;
        this.apiCallName = apiCallName;
        this.descriptionType = descriptionType;
    }

    @Override
    public String getName()
    {
        return apiCallName;
    }

    @Override
    public String getDescription()
    {
        return "Called by the satellite to fetch an update of an object of type " + descriptionType;
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer satellitePeer)
        throws IOException
    {
        MsgIntObjectId objId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID objUuid = UUID.fromString(objId.getUuid());
        String name = objId.getName();

        handleRequest(name, objUuid, msgId, satellitePeer);
    }

    protected abstract void handleRequest(String name, UUID objUuid, int msgId, Peer satellitePeer);

}
