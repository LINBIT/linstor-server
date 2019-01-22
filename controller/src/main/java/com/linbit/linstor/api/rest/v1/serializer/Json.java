package com.linbit.linstor.api.rest.v1.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.api.pojo.NetInterfacePojo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Json
{
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NetInterfaceData
    {
        public String name;
        public String address;
        public Integer stlt_port;
        public String stlt_encryption_type;

        public NetInterfaceData()
        {
        }

        public NetInterfaceData(NetInterface.NetInterfaceApi netIfApi)
        {
            name = netIfApi.getName();
            address = netIfApi.getAddress();
            if (netIfApi.isUsableAsSatelliteConnection())
            {
                stlt_encryption_type = netIfApi.getSatelliteConnectionEncryptionType();
                stlt_port = netIfApi.getSatelliteConnectionPort();
            }
        }

        public NetInterface.NetInterfaceApi toApi()
        {
            return new NetInterfacePojo(null, name, address, stlt_port, stlt_encryption_type);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeData
    {
        public String name;
        public String type;
        public Map<String, String> props;
        public List<String> flags;
        public List<NetInterfaceData> net_interfaces = new ArrayList<>();
        public String connection_status;
    }

    public static class NodeModifyData
    {
        public String node_type;
        public Map<String, String> override_props = Collections.emptyMap();
        public Set<String> delete_props = Collections.emptySet();
        public Set<String> delete_namespaces = Collections.emptySet();
    }
}
