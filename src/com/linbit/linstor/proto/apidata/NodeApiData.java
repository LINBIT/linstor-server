package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Node;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NodeApiData implements Node.NodeApi {

    final private String name;
    final private UUID uuid;
    final private String type;

    public NodeApiData(String name, UUID uuid, String type) {
        this.name = name;
        this.uuid = uuid;
        this.type = type;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String getType()
    {
        return type;
    }

    @Override
    public Map<String, String> getProps() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
