package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceData implements Resource
{
    // Object identifier
    private UUID objId;

    // Reference to the resource definition
    private ResourceDefinition resourceDfn;

    // List of volumes of this resource
    private Map<VolumeNumber, Volume> volumeList;

    // Reference to the node this resource is assigned to
    private Node assgNode;

    // Access control for this resource
    private ObjectProtection objProt;

    ResourceData(AccessContext accCtx, ResourceDefinition resDfnRef, Node nodeRef)
    {
        if (resDfnRef == null || nodeRef == null)
        {
            throw new ImplementationError(
                "Attempt to construct instance with a null argument",
                new NullPointerException()
            );
        }
        resourceDfn = resDfnRef;
        assgNode = nodeRef;

        objId = UUID.randomUUID();
        volumeList = new TreeMap<>();
        objProt = new ObjectProtection(accCtx);
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceDefinition getDefinition()
    {
        return resourceDfn;
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        return volumeList.get(volNr);
    }

    @Override
    public Node getAssignedNode()
    {
        return assgNode;
    }
}
