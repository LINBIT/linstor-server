package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource.Key;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.StorPoolName;

import java.util.Set;
import java.util.UUID;

public class UpdateTrackerImpl implements UpdateTracker
{
//    UpdateBundle toRequestBundle;

    @Override
    public void updateController()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateNode(UUID nodeUuid, NodeName name)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateResource(UUID rscUuid, ResourceName resourceName, NodeName nodeName)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateStorPool(UUID storPoolUuid, StorPoolName storPoolName)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateSnapshot(UUID snapshotUuid, ResourceName resourceName, SnapshotName snapshotName)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void controllerUpdateApplied(Set<ResourceName> rscSet)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void storPoolUpdateApplied(Set<StorPoolName> storPoolSet, Set<ResourceName> rscSet)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void rscUpdateApplied(Set<Key> rscSet)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void snapshotUpdateApplied(Set<com.linbit.linstor.SnapshotDefinition.Key> snapshotKeySet)
    {
        // TODO Auto-generated method stub

    }
}
