package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import java.util.Set;

public interface StltUpdateTracker
{
    void updateNode(NodeName name);
    void updateResourceDfn(ResourceName name);
    void updateResource(ResourceName rscName, Set<NodeName> updNodeSet);
    void updateStorPool(StorPoolName name);
    void checkResource(ResourceName name);
}
