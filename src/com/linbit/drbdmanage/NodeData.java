package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeData implements Node
{
    // Object identifier
    private UUID objId;

    // Node name
    private NodeName clNodeName;

    // List of resources assigned to this cluster node
    private Map<ResourceName, ResourceData> resourceList;

    // Access controls for this object
    private ObjectProtection objProt;

    NodeData(AccessContext accCtx, NodeName nameRef)
    {
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);
        objId = UUID.randomUUID();
        clNodeName = nameRef;
        resourceList = new TreeMap<>();
        objProt = new ObjectProtection(accCtx);
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public NodeName getName()
    {
        return clNodeName;
    }

    @Override
    public Resource getAssignedResource(ResourceName resName)
    {
        return resourceList.get(resName);
    }
}
