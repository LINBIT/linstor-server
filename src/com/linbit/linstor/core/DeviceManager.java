package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import java.util.Map;
import java.util.Set;

public interface DeviceManager
{
    void nodeUpdateApplied(Set<NodeName> nodeSet);
    void rscDefUpdateApplied(Set<ResourceName> rscDfnSet);
    void storPoolUpdateApplied(Set<StorPoolName> storPoolSet);
    void rscUpdateApplied(Map<ResourceName, Set<NodeName>> rscMap);
    void updateApplied(
        Set<NodeName> nodeSet,
        Set<ResourceName> rscDfnSet,
        Set<StorPoolName> storPoolSet,
        Map<ResourceName, Set<NodeName>> rscMap
    );
    StltUpdateTracker getUpdateTracker();
}
