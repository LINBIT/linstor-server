package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgReqRscConnOuterClass.MsgReqRscConn;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_REQ_RSC_CONN_LIST,
    description = "Returns the requestes resource connections",
    transactional = false
)
public class ReqRscConnList implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public ReqRscConnList(CtrlApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgReqRscConn reqRscConn = MsgReqRscConn.parseDelimitedFrom(msgDataIn);

        client.sendMessage(
            apiCallHandler.listResourceConnections(reqRscConn.getRscName())
        );
    }
}
