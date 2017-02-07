package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
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
    private Map<ResourceName, Resource> resourceList;

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
    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceList.get(resName);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
