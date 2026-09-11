package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Holds the singleton nodes map instance.
 */
@Singleton
public class NodeRepositoryImpl implements NodeRepository
{
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.UnameMap uNameMap;

    @Inject
    public NodeRepositoryImpl(CoreModule.NodesMap nodesMapRef, CoreModule.UnameMap uNameMapRef)
    {
        nodesMap = nodesMapRef;
        uNameMap = uNameMapRef;
    }

    @Override
    public @Nullable Node get(
        NodeName nodeName
    )
    {
        return nodesMap.get(nodeName);
    }

    @Override
    public void put(NodeName nodeName, Node node)
    {
        nodesMap.put(nodeName, node);
    }

    @Override
    public void remove(NodeName nodeName)
    {
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
    public CoreModule.NodesMap getMapForView()
    {
        return nodesMap;
    }

    private void removeUnames(Collection<String> uNames)
    {
        for (var uName : uNames)
        {
            uNameMap.remove(uName);
        }
    }

    @Override
    public void putUname(String uName, NodeName nodeName)
    {
        uNameMap.put(uName, nodeName);
    }

    @Override
    public @Nullable NodeName getUname(String uName)
    {
        return uNameMap.get(uName);
    }

    @Override
    public void removeUname(String uName)
    {
        uNameMap.remove(uName);
    }
}
