package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage2.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.storage2.layer.kinds.DeviceLayerKindFactory;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TopDownOrder implements TraverseOrder
{

    private AccessContext sysCtx;

    public TopDownOrder(AccessContext privCtx)
    {
        sysCtx = privCtx;
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
                        // .filter(rsc -> isHighestUnprocessedDependency(rscList, rsc))
                        .collect(Collectors.toList())
                )
            );
        }
        return batches;
    }

    @Override
    public long getProcessableCount(Collection<Resource> rscList, Collection<Resource> resourcesToProcess)
    {
        return rscList.stream()
            .filter(rsc -> isHighestUnprocessedDependency(resourcesToProcess, rsc))
            .count();
    }

    @Override
    public Phase getPhase()
    {
        return Phase.TOP_DOWN;
    }

    private boolean isHighestUnprocessedDependency(Collection<Resource> rscList, Resource rsc)
    {
        boolean ret;
        try
        {
            ret = rsc.equals(getHighestUnprocessedDependencyResource(rsc, rscList));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    private Resource getHighestUnprocessedDependencyResource(
        Resource resource,
        Collection<Resource> resourcesToProcessList
    )
        throws AccessDeniedException
    {
        Resource unprocessedRsc = resource;
        while (true)
        {
            Resource parent = unprocessedRsc.getParentResource(sysCtx);
            if (parent != null && resourcesToProcessList.contains(parent))
            {
                unprocessedRsc = parent;
            }
            else
            {
                break;
            }
        }
        return unprocessedRsc;
    }
}
