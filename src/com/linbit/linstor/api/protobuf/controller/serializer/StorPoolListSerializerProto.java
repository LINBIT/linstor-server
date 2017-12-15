/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.protobuf.controller.serializer;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolOuterClass;
import com.linbit.linstor.proto.apidata.StorPoolApiData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public class StorPoolListSerializerProto implements CtrlListSerializer<StorPool.StorPoolApi>{

    @Override
    public byte[] getListMessage(int msgId, List<StorPool.StorPoolApi> elements) throws IOException {
        MsgLstStorPoolOuterClass.MsgLstStorPool.Builder msgListBuilder =
                MsgLstStorPoolOuterClass.MsgLstStorPool.newBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_LST_STOR_POOL)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);

        for (StorPool.StorPoolApi apiStorPool: elements)
        {
            msgListBuilder.addStorPools(StorPoolApiData.toStorPoolProto(apiStorPool));
        }

        msgListBuilder.build().writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
