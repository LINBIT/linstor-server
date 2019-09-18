package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton nodes map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class NodeProtectionRepository implements NodeRepository
{
    private final CoreModule.NodesMap nodesMap;
    private ObjectProtection nodesMapObjProt;

    @Inject
    public NodeProtectionRepository(CoreModule.NodesMap nodesMapRef)
    {
        nodesMap = nodesMapRef;
    }

    public void setObjectProtection(ObjectProtection nodesMapObjProtRef)
    {
        if (nodesMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        nodesMapObjProt = nodesMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return nodesMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public Node get(
        AccessContext accCtx,
        NodeName nodeName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return (Node) nodesMap.get(nodeName);
    }

    @Override
    public void put(AccessContext accCtx, NodeName nodeName, Node node)
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        nodesMap.put(nodeName, node);
    }

    @Override
    public void remove(AccessContext accCtx, NodeName nodeName)
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        nodesMap.remove(nodeName);
    }

    @Override
    public CoreModule.NodesMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return nodesMap;
    }

    private void checkProtSet()
    {
        if (nodesMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
