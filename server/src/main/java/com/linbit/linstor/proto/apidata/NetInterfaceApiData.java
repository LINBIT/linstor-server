package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.annotation.Nullable;
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

    @Override
    public String getSatelliteConnectionEncryptionType()
    {
        return netInterface.getStltEncryptionType();
    }

    @Override
    public Integer getSatelliteConnectionPort()
    {
        return netInterface.getStltPort();
    }

    @Override
    public boolean isUsableAsSatelliteConnection()
    {
        return netInterface.hasStltEncryptionType() && netInterface.hasStltPort();
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
        bld.setUuid(netInterApi.getUuid().toString());
        bld.setName(netInterApi.getName());
        bld.setAddress(netInterApi.getAddress());
        if (netInterApi.isUsableAsSatelliteConnection())
        {
            bld.setStltPort(netInterApi.getSatelliteConnectionPort());
            bld.setStltEncryptionType(netInterApi.getSatelliteConnectionEncryptionType());
        }
        return bld.build();
    }
}
