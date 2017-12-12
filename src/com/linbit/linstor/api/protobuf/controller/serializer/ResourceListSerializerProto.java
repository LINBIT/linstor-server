package com.linbit.linstor.api.protobuf.controller.serializer;

import com.linbit.linstor.Resource;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstRscOuterClass;
import com.linbit.linstor.proto.apidata.RscApiData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public class ResourceListSerializerProto implements CtrlListSerializer<Resource.RscApi> {

    @Override
    public byte[] getListMessage(int msgId, List<Resource.RscApi> elements) throws IOException {
        MsgLstRscOuterClass.MsgLstRsc.Builder msgListRscsBuilder = MsgLstRscOuterClass.MsgLstRsc.newBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_LST_RSC)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);

        for(Resource.RscApi apiRsc: elements)
        {
            msgListRscsBuilder.addResources(RscApiData.toRscProto(apiRsc));
        }

        msgListRscsBuilder.build().writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
