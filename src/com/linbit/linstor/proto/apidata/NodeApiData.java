package com.linbit.linstor.proto.apidata;

import com.google.protobuf.ByteString;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NodeApiData implements Node.NodeApi {
    private NodeOuterClass.Node node;

    public NodeApiData(NodeOuterClass.Node nodeRef)
    {
        node = nodeRef;
    }

    @Override
    public String getName()
    {
        return node.getName();
    }

    @Override
    public UUID getUuid() {
        UUID uuid = null;
        if(node.hasUuid())
        {
            uuid = UUID.nameUUIDFromBytes(node.getUuid().toByteArray());
        }
        return uuid;
    }

    @Override
    public String getType()
    {
        return node.getType();
    }

    @Override
    public Map<String, String> getProps() {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : node.getPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    public static NodeOuterClass.Node fromNodeApi(Node.NodeApi nodeApi)
    {
        NodeOuterClass.Node.Builder bld = NodeOuterClass.Node.newBuilder();

        bld.setName(nodeApi.getName());
        bld.setType(nodeApi.getType());
        bld.setUuid(ByteString.copyFrom(nodeApi.getUuid().toString().getBytes()));
        bld.addAllProps(BaseProtoApiCall.fromMap(nodeApi.getProps()));

        return bld.build();
    }

}
