package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class ListStorPoolDefinition extends BaseProtoApiCall {
    private final Controller controller;

    public ListStorPoolDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_LST_STOR_POOL_DFN;
    }

    @Override
    public String getDescription()
    {
        return "List storage pool definitions";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {

        byte[] listResultMsg = controller.getApiCallHandler().listStorPoolDefinition(msgId, accCtx, client);
        sendAnswer(client, listResultMsg);
    }
}
