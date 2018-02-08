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
import com.linbit.linstor.proto.MsgModVlmConnOuterClass.MsgModVlmConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyVolumeConn extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyVolumeConn(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_VLM_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a volume connection";
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
        MsgModVlmConn msgModVlmConn = MsgModVlmConn.parseDelimitedFrom(msgDataIn);
        UUID vlmConnUuid = null;
        if (msgModVlmConn.hasVlmConnUuid())
        {
            vlmConnUuid = UUID.fromString(msgModVlmConn.getVlmConnUuid());
        }
        String nodeName1 = msgModVlmConn.getNode1Name();
        String nodeName2 = msgModVlmConn.getNode2Name();
        String rscName = msgModVlmConn.getRscName();
        int vlmNr = msgModVlmConn.getVlmNr();
        Map<String, String> overrideProps = asMap(msgModVlmConn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModVlmConn.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyVlmConn(
            accCtx,
            client,
            vlmConnUuid,
            nodeName1,
            nodeName2,
            rscName,
            vlmNr,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
