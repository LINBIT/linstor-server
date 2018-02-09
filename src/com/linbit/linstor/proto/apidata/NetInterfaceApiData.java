package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.NetInterface;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfaceApiData implements NetInterface.NetInterfaceApi
{
    private final NetInterfaceOuterClass.NetInterface netInterface;

    public NetInterfaceApiData(final NetInterfaceOuterClass.NetInterface refNetInterface)
    {
        netInterface = refNetInterface;
    }

    @Override
    public UUID getUuid()
    {
        UUID ifUuid = null;
        if (netInterface.hasUuid())
        {
            ifUuid = UUID.fromString(netInterface.getUuid());
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

    public static List<NetInterfaceOuterClass.NetInterface> toNetInterfaceProtoList(
        List<NetInterface.NetInterfaceApi> netInterfaceApiList)
    {
        ArrayList<NetInterfaceOuterClass.NetInterface> resultList = new ArrayList<>();
        for (NetInterface.NetInterfaceApi netInterApi : netInterfaceApiList)
        {
            resultList.add(toNetInterfaceProto(netInterApi));
        }
        return resultList;
    }

    public static NetInterfaceOuterClass.NetInterface toNetInterfaceProto(
            final NetInterface.NetInterfaceApi netInterApi)
    {
        NetInterfaceOuterClass.NetInterface.Builder bld = NetInterfaceOuterClass.NetInterface.newBuilder();
        bld.setUuid(netInterApi.getUuid().toString());
        bld.setName(netInterApi.getName());
        bld.setAddress(netInterApi.getAddress());
        return bld.build();
    }
}
