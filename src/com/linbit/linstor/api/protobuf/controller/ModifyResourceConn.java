package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModRscConnOuterClass.MsgModRscConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyResourceConn extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyResourceConn(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_RSC_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a resource connection";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgModRscConn msgModRscConn = MsgModRscConn.parseDelimitedFrom(msgDataIn);
        UUID rscConnUuid = null;
        if (msgModRscConn.hasRscConnUuid())
        {
            rscConnUuid = UUID.fromString(msgModRscConn.getRscConnUuid());
        }
        String nodeName1 = msgModRscConn.getNode1Name();
        String nodeName2 = msgModRscConn.getNode2Name();
        String rscName = msgModRscConn.getRscName();
        Map<String, String> overrideProps = asMap(msgModRscConn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModRscConn.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyRscConn(
            accCtx,
            client,
            rscConnUuid,
            nodeName1,
            nodeName2,
            rscName,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
