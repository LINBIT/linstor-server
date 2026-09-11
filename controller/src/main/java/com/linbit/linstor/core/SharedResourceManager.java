package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Finds resources whose backing data lives in the same shared storage pool. In contrast to
 * {@link SharedStorPoolManager}, which manages the LINSTOR-internal locks and therefore ignores
 * storage pools with external locking (e.g. lvmlockd), the data of two storage pools is shared
 * whenever they have the same shared storage pool name - no matter who manages the locks.
 */
@Singleton
public class SharedResourceManager
{

    @Inject
    public SharedResourceManager()
    {
    }

    public boolean isBackedBySharedStorPool(Resource rsc)
    {
        return !getDataSharedSpNames(LayerVlmUtils.getStorPools(rsc)).isEmpty();
    }

    public boolean isBackedBySharedStorPool(Snapshot snap)
    {
        return !getDataSharedSpNames(LayerVlmUtils.getStorPools(snap)).isEmpty();
    }

    /**
     * Returns the shared storage pool names of the pools backing the given snapshot - i.e. the
     * shared spaces holding the snapshot's data. Empty for snapshots without shared storage pools.
     */
    public Set<SharedStorPoolName> getSharedSpNames(Snapshot snap)
    {
        return getDataSharedSpNames(LayerVlmUtils.getStorPools(snap));
    }

    /**
     * Returns the shared storage pool names of the pools backing the given snapshot whose shared
     * data is serialized by an external lock manager (e.g. lvmlockd) instead of LINSTOR's own
     * shared storage pool locks. Empty for snapshots on non-shared or LINSTOR-locked pools.
     */
    public Set<SharedStorPoolName> getExternallyLockedSpNames(Snapshot snap)
    {
        Set<SharedStorPoolName> ret = new TreeSet<>();
        for (StorPool sp : LayerVlmUtils.getStorPools(snap))
        {
            if (sp.isShared() && !sp.usesLinstorLocking())
            {
                ret.add(sp.getSharedStorPoolName());
            }
        }
        return ret;
    }

    /**
     * Returns a per-node snapshot of the given snapshot-definition that is backed by a shared
     * storage pool the given resource also uses - i.e. a snapshot whose data exists on shared data
     * the resource's node has access to. Null if there is no such snapshot, e.g. because the
     * snapshot was taken on a different shared space or on a node without shared storage pools.
     * The check is based on the snapshots' storage pools rather than on sibling resources, so it
     * also matches when the resource the snapshot was created from no longer exists.
     */
    public @Nullable Snapshot findSnapshotOnSharedSp(SnapshotDefinition snapDfn, Resource rsc)
    {
        @Nullable Snapshot ret = null;
        Set<SharedStorPoolName> rscSharedNames = getDataSharedSpNames(LayerVlmUtils.getStorPools(rsc));
        if (!rscSharedNames.isEmpty())
        {
            for (Snapshot snap : new TreeSet<>(snapDfn.getAllSnapshots()))
            {
                Set<SharedStorPoolName> snapSharedNames = getSharedSpNames(snap);
                snapSharedNames.retainAll(rscSharedNames);
                if (!snapSharedNames.isEmpty())
                {
                    ret = snap;
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * Returns true if the given resource is an inactive copy of a shared storage pool, i.e. a copy that
     * does not currently use the shared data.
     */
    public boolean isInactiveShared(Resource rsc)
    {
        return rsc.getStateFlags().isSomeSet(
                Resource.Flags.INACTIVE,
                Resource.Flags.INACTIVE_PERMANENTLY
            ) &&
            isBackedBySharedStorPool(rsc);
    }

    public boolean isActivationAllowed(Resource rsc)
    {
        boolean ret = true;
        Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc);
        Set<SharedStorPoolName> sharedSpNames = getDataSharedSpNames(storPools);

        Iterator<Resource> rscIt = rsc.getResourceDefinition().iterateResource();
        while (rscIt.hasNext())
        {
            Resource tmpRsc = rscIt.next();
            if (!tmpRsc.equals(rsc))
            {
                Set<StorPool> tmpStorPools = LayerVlmUtils.getStorPools(tmpRsc);
                Set<SharedStorPoolName> tmpSharedSpNames = getDataSharedSpNames(tmpStorPools);

                tmpSharedSpNames.retainAll(sharedSpNames);
                if (!tmpSharedSpNames.isEmpty())
                {
                    // rsc shares at least one storPool with tmpRsc.
                    // rsc can only get active if tmpRsc is inactive
                    boolean isTmpRscInactive = tmpRsc.getStateFlags()
                        .isSomeSet(
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

        return ret;
    }

    public TreeSet<Resource> getSharedResources(Resource rsc)
    {
        TreeSet<Resource> result;
        Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc);
        Set<SharedStorPoolName> sharedSpNames = getDataSharedSpNames(storPools);

        result = getSharedResources(sharedSpNames, rsc.getResourceDefinition());
        result.remove(rsc);

        return result;
    }

    public TreeSet<Resource> getSharedResources(Set<SharedStorPoolName> sharedSpNames, ResourceDefinition rscDfn)
    {
        TreeSet<Resource> result = new TreeSet<>();


        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource tmpRsc = rscIt.next();
            Set<StorPool> tmpStorPools = LayerVlmUtils.getStorPools(tmpRsc);
            Set<SharedStorPoolName> tmpSharedSpNames = getDataSharedSpNames(tmpStorPools);

            tmpSharedSpNames.retainAll(sharedSpNames);
            if (!tmpSharedSpNames.isEmpty())
            {
                result.add(tmpRsc);
            }
        }

        return result;
    }

    private static Set<SharedStorPoolName> getDataSharedSpNames(Set<StorPool> storPools)
    {
        Set<SharedStorPoolName> ret = new TreeSet<>();
        for (StorPool sp : storPools)
        {
            if (sp.isShared())
            {
                ret.add(sp.getSharedStorPoolName());
            }
        }
        return ret;
    }
}
