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
import com.linbit.utils.UuidUtils;

abstract class RequestObject extends BaseProtoApiCall
{
    protected Controller controller;
    private String apiCallName;
    private String descriptionType;

    public RequestObject(
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
        return "This request is answered with a full data response of the requested " + descriptionType;
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
        UUID objUuid = UuidUtils.asUuid(objId.getUuid().toByteArray());
        String name = objId.getName();

        handleRequest(name, objUuid, msgId, satellitePeer);
    }

    protected abstract void handleRequest(String name, UUID objUuid, int msgId, Peer satellitePeer);

}
