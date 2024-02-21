package com.linbit.linstor.core.apicallhandler.controller.internal.helpers;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AtomicUpdateSatelliteData
{
    // TODO extend this class with ctrl-props, nodes, storpools, ...

    private final ArrayList<ResourceDefinition> rscDfnsToUpdate = new ArrayList<>();
    private final ArrayList<SnapshotDefinition> snapDfnsToUpdate = new ArrayList<>();

    public AtomicUpdateSatelliteData()
    {
    }

    public Collection<Node> getInvolvedOnlineNodes(AccessContext accCtx) throws AccessDeniedException
    {
        Set<Node> nodes = new HashSet<>();
        for (ResourceDefinition rscDfn : rscDfnsToUpdate)
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                Node node = rsc.getNode();
                if (node.getPeer(accCtx).isOnline())
                {
                    nodes.add(rsc.getNode());
                }
            }
        }
        for (SnapshotDefinition snapDfn : snapDfnsToUpdate)
        {
            for (Snapshot snapshot : snapDfn.getAllSnapshots(accCtx))
            {
                nodes.add(snapshot.getNode());
            }
        }

        // TODO extend this method when adding nodes, storpools, ...
        return nodes;
    }

    public ArrayList<ResourceDefinition> getRscDfnsToUpdate()
    {
        return rscDfnsToUpdate;
    }

    public ArrayList<SnapshotDefinition> getSnapDfnsToUpdate()
    {
        return snapDfnsToUpdate;
    }

    public AtomicUpdateSatelliteData add(ResourceDefinition rscDfn)
    {
        rscDfnsToUpdate.add(rscDfn);
        return this;
    }

    public AtomicUpdateSatelliteData add(SnapshotDefinition snapDfn)
    {
        snapDfnsToUpdate.add(snapDfn);
        rscDfnsToUpdate.add(snapDfn.getResourceDefinition());
        return this;
    }

    public AtomicUpdateSatelliteData addSnapDfns(Collection<SnapshotDefinition> snapDfns)
    {
        for (SnapshotDefinition snapDfn : snapDfns)
        {
            snapDfnsToUpdate.add(snapDfn);
            rscDfnsToUpdate.add(snapDfn.getResourceDefinition());
        }
        return this;
    }
}
