package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
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
public class ResourceDefinitionData implements ResourceDefinition
{
    // Object identifier
    private UUID objId;

    // Resource name
    private ResourceName resourceName;

    // Connections to the peer resources
    private Map<NodeName, Map<Integer, ConnectionDefinition>> connectionMap;

    // Volumes of the resource
    private Map<VolumeNumber, VolumeDefinition> volumeMap;

    // Object access controls
    private ObjectProtection objProt;

    ResourceDefinitionData(
        AccessContext accCtx,
        ResourceName resName
    )
    {
        if (resName == null)
        {
            throw new ImplementationError(
                "Attempt to construct an instance with a null argument",
                new NullPointerException()
            );
        }
        objId = UUID.randomUUID();
        resourceName = resName;
        connectionMap = new TreeMap<>();
        volumeMap = new TreeMap<>();
        objProt = new ObjectProtection(accCtx);
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        return resourceName;
    }

    @Override
    public ConnectionDefinition getConnectionDfn(AccessContext accCtx, NodeName clNodeName, Integer connNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        ConnectionDefinition connDfn = null;
        Map<Integer, ConnectionDefinition> nodeConnMap = connectionMap.get(clNodeName);
        if (nodeConnMap != null)
        {
            connDfn = nodeConnMap.get(connNr);
        }
        return connDfn;
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }
}
