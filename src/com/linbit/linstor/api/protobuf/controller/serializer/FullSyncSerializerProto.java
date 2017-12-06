package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.interfaces.serializer.CtrlFullSyncSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessDeniedException;

public class FullSyncSerializerProto implements CtrlFullSyncSerializer
{
    private final ErrorReporter errorReporter;
    private final NodeDataSerializerProto nodeSerializer;
    private final ResourceDataSerializerProto rscSerializer;
    private final StorPoolDataSerializerProto storPoolSerializer;

    public FullSyncSerializerProto(
        ErrorReporter errorReporterRef,
        NodeDataSerializerProto nodeSerializerRef,
        ResourceDataSerializerProto rscSerializerRef,
        StorPoolDataSerializerProto storPoolSerializerRef
    )
    {
        errorReporter = errorReporterRef;
        nodeSerializer = nodeSerializerRef;
        rscSerializer = rscSerializerRef;
        storPoolSerializer = storPoolSerializerRef;
    }

    @Override
    public byte[] getData(
        int msgId,
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources
    )
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] toSend;

        try
        {
            ArrayList<MsgIntNodeData> serializedNodes = new ArrayList<>();
            ArrayList<MsgIntStorPoolData> serializedStorPools = new ArrayList<>();
            ArrayList<MsgIntRscData> serializedRscs = new ArrayList<>();

            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_FULL_SYNC_DATA)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);
            LinkedList<Node> nodes = new LinkedList<Node>(nodeSet);
            while (!nodes.isEmpty())
            {
                Node node = nodes.removeFirst();
                serializedNodes.add(nodeSerializer.buildProtoMsg(node, nodes));
            }
            for (StorPool storPool : storPools)
            {
                serializedStorPools.add(storPoolSerializer.buildData(storPool));
            }
            for (Resource rsc : resources)
            {
                serializedRscs.add(rscSerializer.buildData(rsc));
            }

            MsgIntFullSync.newBuilder()
                .addAllNodes(serializedNodes)
                .addAllStorPools(serializedStorPools)
                .addAllRscs(serializedRscs)
                .build()
                .writeDelimitedTo(baos);

            toSend = baos.toByteArray();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "FullSyncSerializer's accessContext does not have enough privileges to serialize satellite sync",
                    accDeniedExc
                )
            );
            toSend = new byte[0];
        }
        return toSend;
    }
}
