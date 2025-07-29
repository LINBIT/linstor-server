package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.proto.common.NetInterfaceOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfaceApiData implements NetInterfaceApi
{
    private final NetInterfaceOuterClass.NetInterface netInterface;

    public NetInterfaceApiData(final NetInterfaceOuterClass.NetInterface refNetInterface)
    {
        netInterface = refNetInterface;
    }

    @Override
    public @Nullable UUID getUuid()
    {
        UUID ifUuid = null;
        if (netInterface.hasUuid())
        {
            ifUuid = ProtoUuidUtils.deserialize(netInterface.getUuid());
        }
        return ifUuid;
    }

    @Override
    public String getName()
    {
        return netInterface.getName();
    }

    @Override
    public String getAddress()
    {
        return netInterface.getAddress();
    }

    @Override
    public @Nullable StltConn getStltConn()
    {
        return netInterface.hasStltConn() ? new StltConn(
            netInterface.getStltConn().getStltPort(),
            netInterface.getStltConn().getStltEncryptionType()
        ) : null;
    }

    public static List<NetInterfaceOuterClass.NetInterface> toNetInterfaceProtoList(
        List<NetInterfaceApi> netInterfaceApiList)
    {
        ArrayList<NetInterfaceOuterClass.NetInterface> resultList = new ArrayList<>();
        for (NetInterfaceApi netInterApi : netInterfaceApiList)
        {
            resultList.add(toNetInterfaceProto(netInterApi));
        }
        return resultList;
    }

    public static NetInterfaceOuterClass.NetInterface toNetInterfaceProto(
            final NetInterfaceApi netInterApi)
    {
        NetInterfaceOuterClass.NetInterface.Builder bld = NetInterfaceOuterClass.NetInterface.newBuilder();
        bld.setUuid(ProtoUuidUtils.serialize(netInterApi.getUuid()));
        bld.setName(netInterApi.getName());
        bld.setAddress(netInterApi.getAddress());
        @Nullable StltConn stltConn = netInterApi.getStltConn();
        if (stltConn != null)
        {
            NetInterfaceOuterClass.StltConn.Builder connBld = NetInterfaceOuterClass.StltConn.newBuilder();
            connBld.setStltPort(stltConn.getSatelliteConnectionPort());
            connBld.setStltEncryptionType(stltConn.getSatelliteConnectionEncryptionType());
            bld.setStltConn(connBld.build());
        }
        return bld.build();
    }
}
