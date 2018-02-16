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
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModStorPoolOuterClass.MsgModStorPool;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyStorPool extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyStorPool(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_STOR_POOL;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a storage pool";
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
        MsgModStorPool msgModStorPool = MsgModStorPool.parseDelimitedFrom(msgDataIn);
        UUID storPoolUuid = null;
        if (msgModStorPool.hasStorPoolUuid())
        {
            storPoolUuid = UUID.fromString(msgModStorPool.getStorPoolUuid());
        }
        String nodeName = msgModStorPool.getNodeName();
        String storPoolName = msgModStorPool.getStorPoolName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModStorPool.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModStorPool.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyStorPool(
            accCtx,
            client,
            storPoolUuid,
            nodeName,
            storPoolName,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
