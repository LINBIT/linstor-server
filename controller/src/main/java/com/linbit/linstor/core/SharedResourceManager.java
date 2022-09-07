package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class SharedResourceManager
{
    private final AccessContext sysCtx;

    @Inject
    public SharedResourceManager(@SystemContext AccessContext sysCtxRef)
    {
        sysCtx = sysCtxRef;
    }

    public boolean isActivationAllowed(Resource rsc)
    {
        boolean ret = true;
        try
        {
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, sysCtx);
            Set<SharedStorPoolName> sharedSpNames = SharedStorPoolManager.getSharedSpNames(storPools);

            Iterator<Resource> rscIt = rsc.getResourceDefinition().iterateResource(sysCtx);
            while (rscIt.hasNext())
            {
                Resource tmpRsc = rscIt.next();
                if (tmpRsc != rsc)
                {
                    Set<StorPool> tmpStorPools = LayerVlmUtils.getStorPools(tmpRsc, sysCtx);
                    Set<SharedStorPoolName> tmpSharedSpNames = SharedStorPoolManager.getSharedSpNames(tmpStorPools);

                    tmpSharedSpNames.retainAll(sharedSpNames);
                    if (!tmpSharedSpNames.isEmpty())
                    {
                        // rsc shares at least one storPool with tmpRsc.
                        // rsc can only get active if tmpRsc is inactive
                        boolean isTmpRscInactive = tmpRsc.getStateFlags()
                            .isSomeSet(
                                sysCtx,
                                Resource.Flags.INACTIVE,
                                Resource.Flags.INACTIVE_PERMANENTLY
                            );
                        if (!isTmpRscInactive)
                        {
                            ret = false;
                            break;
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return ret;
    }

    public TreeSet<Resource> getSharedResources(Resource rsc)
    {
        TreeSet<Resource> result;
        try
        {
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, sysCtx);
            Set<SharedStorPoolName> sharedSpNames = SharedStorPoolManager.getSharedSpNames(storPools);

            result = getSharedResources(sharedSpNames, rsc.getResourceDefinition());
            result.remove(rsc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return result;
    }

    public TreeSet<Resource> getSharedResources(Set<SharedStorPoolName> sharedSpNames, ResourceDefinition rscDfn)
    {
        TreeSet<Resource> result = new TreeSet<>();

        try
        {

            Iterator<Resource> rscIt = rscDfn.iterateResource(sysCtx);
            while (rscIt.hasNext())
            {
                Resource tmpRsc = rscIt.next();
                Set<StorPool> tmpStorPools = LayerVlmUtils.getStorPools(tmpRsc, sysCtx);
                Set<SharedStorPoolName> tmpSharedSpNames = SharedStorPoolManager.getSharedSpNames(tmpStorPools);

                tmpSharedSpNames.retainAll(sharedSpNames);
                if (!tmpSharedSpNames.isEmpty())
                {
                    result.add(tmpRsc);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

        return result;
    }
}
