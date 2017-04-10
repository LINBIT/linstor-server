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
    private Map<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private Map<NetInterfaceName, NetInterface> netInterfaceMap;

    // Access controls for this object
    private ObjectProtection objProt;

    NodeData(AccessContext accCtx, NodeName nameRef)
    {
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);
        objId = UUID.randomUUID();
        clNodeName = nameRef;
        resourceMap = new TreeMap<>();
        netInterfaceMap = new TreeMap<>();
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
        return resourceMap.get(resName);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getDefinition().getName(), resRef);
    }

    @Override
    public void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getDefinition().getName());
    }

    @Override
    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return netInterfaceMap.get(niName);
    }

    @Override
    public void addNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.put(niRef.getName(), niRef);
    }

    @Override
    public void removeNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.remove(niRef.getName());
    }
}
