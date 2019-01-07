package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.kinds.DeviceLayerKindFactory;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class BottomUpOrder implements TraverseOrder
{
    private AccessContext sysCtx;

    public BottomUpOrder(AccessContext sysCtxRef)
    {
        sysCtx = sysCtxRef;
    }

    @Override
    public List<Pair<DeviceLayerKind, List<Resource>>> getAllBatches(Collection<Resource> rscList)
    {
        List<Pair<DeviceLayerKind, List<Resource>>> batches = new ArrayList<>();
        for (DeviceLayerKind kind : DeviceLayerKindFactory.getKinds())
        {
            batches.add(
                new Pair<>(
                    kind,
                    rscList.stream()
                        .filter(rsc -> rsc.getType().getDevLayerKind().getClass().equals(kind.getClass()))
//                        .filter(rsc -> isLowestUnprocessedDependency(rscList, rsc))
                        .collect(Collectors.toList())
                )
            );
        }
        return batches;
    }

    @Override
    public long getProcessableCount(Collection<Resource> rscList, Collection<Resource> resourcesToProcress)
    {
        return rscList.stream()
            .filter(rsc -> isLowestUnprocessedDependency(resourcesToProcress, rsc))
            .count();
    }

    @Override
    public Phase getPhase()
    {
        return Phase.BOTTOM_UP;
    }

    private boolean isLowestUnprocessedDependency(Collection<Resource> rscList, Resource rsc)
    {
        boolean ret;
        try
        {
            ret = rsc.equals(getLowestUnprocessedDependencyResource(rsc, rscList, new HashSet<>()));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    private Resource getLowestUnprocessedDependencyResource(
        Resource resource,
        Collection<Resource> resourcesToProcessList,
        HashSet<Resource> checkedResources
    )
        throws AccessDeniedException
    {
        Resource unprocessedRsc = resource;
        for (Resource child : resource.getChildResources(sysCtx))
        {
            if (checkedResources.add(child))
            {
                if (resourcesToProcessList.contains(child))
                {
                    unprocessedRsc = getLowestUnprocessedDependencyResource(
                        child,
                        resourcesToProcessList,
                        checkedResources
                    );
                    break;
                }
            }
            else
            {
                throw new ImplementationError("Cyclic parent-child relation: " + child);
            }
        }
        return unprocessedRsc;
    }
}
