package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Holds the singleton nodes map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class NodeProtectionRepository implements NodeRepository
{
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.UnameMap uNameMap;
    private @Nullable ObjectProtection nodesMapObjProt;

    @Inject
    public NodeProtectionRepository(CoreModule.NodesMap nodesMapRef, CoreModule.UnameMap uNameMapRef)
    {
        nodesMap = nodesMapRef;
        uNameMap = uNameMapRef;
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
    public @Nullable Node get(
        AccessContext accCtx,
        NodeName nodeName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return nodesMap.get(nodeName);
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

        ArrayList<String> uNamesToRemove = new ArrayList<>();
        for (var unameEntry : uNameMap.entrySet())
        {
            if (unameEntry.getValue().equals(nodeName))
            {
                uNamesToRemove.add(unameEntry.getKey());
            }
        }
        removeUnames(uNamesToRemove);
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

    private void removeUnames(Collection<String> uNames)
    {
        for (var uName : uNames)
        {
            uNameMap.remove(uName);
        }
    }

    @Override
    public void putUname(AccessContext accCtx, String uName, NodeName nodeName) throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        uNameMap.put(uName, nodeName);
    }

    @Override
    public @Nullable NodeName getUname(AccessContext accCtx, String uName) throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return uNameMap.get(uName);
    }

    @Override
    public void removeUname(AccessContext accCtx, String uName) throws AccessDeniedException
    {
        checkProtSet();
        nodesMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        uNameMap.remove(uName);
    }
}
