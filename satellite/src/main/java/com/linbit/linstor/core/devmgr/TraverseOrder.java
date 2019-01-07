package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.Resource;
import com.linbit.linstor.storage.layer.kinds.DeviceLayerKind;
import com.linbit.utils.Pair;

import java.util.Collection;
import java.util.List;

public interface TraverseOrder
{
    enum Phase
    {
        TOP_DOWN,
        BOTTOM_UP
    }

    /**
     * A simple list where {@link Resource}s are group by their {@link DeviceLayerKind}.
     */
    List<Pair<DeviceLayerKind, List<Resource>>> getAllBatches(Collection<Resource> rscList);

    /**
     * Returns the count of processable resources of the given list.
     * A resource is processable if it has no unprocessed dependency and is in the <code>resourcesToProess</code> list
     * @param rscList
     * @param resourcesToProcess
     * @return
     */
    long getProcessableCount(Collection<Resource> rscList, Collection<Resource> resourcesToProcess);

    Phase getPhase();
}
