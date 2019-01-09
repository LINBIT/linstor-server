package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Node;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.NodeOuterClass;

/**
 *
 * @author rpeinthor
 */
public class NodeApiData
{

    public static NodeOuterClass.Node toNodeProto(Node.NodeApi nodeApi)
    {
        NodeOuterClass.Node.Builder bld = NodeOuterClass.Node.newBuilder();

        bld.setName(nodeApi.getName());
        bld.setType(nodeApi.getType());
        bld.setUuid(nodeApi.getUuid().toString());
        bld.addAllProps(ProtoMapUtils.fromMap(nodeApi.getProps()));
        bld.addAllFlags(Node.NodeFlag.toStringList(nodeApi.getFlags()));
        bld.addAllNetInterfaces(NetInterfaceApiData.toNetInterfaceProtoList(nodeApi.getNetInterfaces()));
        bld.setConnectionStatus(NodeOuterClass.Node.ConnectionStatus.forNumber(nodeApi.connectionStatus().value()));

        return bld.build();
    }
}
