package com.linbit.linstor.api.pojo;

import java.util.Collections;
import java.util.List;

@Deprecated(forRemoval = true)
public class ExosConnectionMapPojo
{
    private final String nodeName;
    private final String enclosureName;
    private final List<String> connections;

    public ExosConnectionMapPojo(String nodeNameRef, String enclosureNameRef, List<String> connectionsRef)
    {
        nodeName = nodeNameRef;
        enclosureName = enclosureNameRef;
        connections = Collections.unmodifiableList(connectionsRef);
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public String getEnclosureName()
    {
        return enclosureName;
    }

    public List<String> getConnections()
    {
        return connections;
    }
}
