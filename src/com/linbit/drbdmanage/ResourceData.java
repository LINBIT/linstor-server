package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
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

    private ResourceData(AccessContext accCtx, ResourceDefinition resDfnRef, Node nodeRef)
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

    @Override
    public Resource create(AccessContext accCtx, ResourceDefinition resDfnRef, Node nodeRef)
        throws AccessDeniedException
    {
        ErrorCheck.ctorNotNull(Resource.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(Resource.class, Node.class, nodeRef);

        Resource newRes = new ResourceData(accCtx, resDfnRef, nodeRef);

        // Access controls on the node and resource must not change
        // while the transaction is in progress
        synchronized (nodeRef)
        {
            synchronized (resDfnRef)
            {
                nodeRef.addResource(accCtx, newRes);
                try
                {
                    resDfnRef.addResource(accCtx, newRes);
                }
                catch (AccessDeniedException accExc)
                {
                    // Rollback adding the resource to the node
                    nodeRef.removeResource(accCtx, newRes);
                }
            }
        }

        return newRes;
    }
}
