package com.linbit.linstor.api.protobuf.controller.serializer;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgLstRscDfnOuterClass;
import com.linbit.linstor.proto.apidata.RscDfnApiData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public class ResourceDefinitionListSerializerProto implements CtrlListSerializer<ResourceDefinition.RscDfnApi> {

    @Override
    public byte[] getListMessage(int msgId, List<ResourceDefinition.RscDfnApi> elements) throws IOException {
        MsgLstRscDfnOuterClass.MsgLstRscDfn.Builder msgListRscDfnsBuilder = MsgLstRscDfnOuterClass.MsgLstRscDfn.newBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_LST_RSC_DFN)
            .setMsgId(msgId)
            .build()
            .writeDelimitedTo(baos);

        for(ResourceDefinition.RscDfnApi apiRscDfn: elements)
        {
            msgListRscDfnsBuilder.addRscDfns(RscDfnApiData.fromRscDfnApi(apiRscDfn));
        }

        msgListRscDfnsBuilder.build().writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
