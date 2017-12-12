/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.protobuf.controller.serializer;

import com.linbit.linstor.Node;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstNodeOuterClass;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.proto.apidata.NodeApiData;

/**
 *
 * @author rpeinthor
 */
public class NodeListSerializerProto implements CtrlListSerializer<Node.NodeApi> {

    @Override
    public byte[] getListMessage(int msgId, List<Node.NodeApi> nodes) throws IOException {
        MsgLstNodeOuterClass.MsgLstNode.Builder msgListNodeBuilder = MsgLstNodeOuterClass.MsgLstNode.newBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_LST_NODE)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);

        for(Node.NodeApi apiNode: nodes)
        {
            msgListNodeBuilder.addNodes(NodeApiData.toNodeProto(apiNode));
        }

        msgListNodeBuilder.build().writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
