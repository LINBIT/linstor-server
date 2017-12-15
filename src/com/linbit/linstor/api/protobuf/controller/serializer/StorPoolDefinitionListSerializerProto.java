/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.protobuf.controller.serializer;

import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolDfnOuterClass;
import com.linbit.linstor.proto.apidata.StorPoolDfnApiData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public class StorPoolDefinitionListSerializerProto implements CtrlListSerializer<StorPoolDefinition.StorPoolDfnApi> {

    @Override
    public byte[] getListMessage(int msgId, List<StorPoolDefinition.StorPoolDfnApi> elements) throws IOException {
        MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.Builder msgListStorPoolDfnsBuilder =
                MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.newBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_LST_STOR_POOL_DFN)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);

        for (StorPoolDefinition.StorPoolDfnApi apiDfn: elements)
        {
            msgListStorPoolDfnsBuilder.addStorPoolDfns(StorPoolDfnApiData.fromStorPoolDfnApi(apiDfn));
        }

        msgListStorPoolDfnsBuilder.build().writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}