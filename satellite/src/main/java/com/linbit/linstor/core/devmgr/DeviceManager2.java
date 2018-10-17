package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.Node;
import com.linbit.linstor.core.StltUpdateTracker;

public interface DeviceManager2
{
    void setLocalNode(Node localNodeRef);

    StltUpdateTracker getUpdateTracker();

//    void markForReqUpdateCtrl();
//    void markForReqUpdate(Node... node);
//    void markForReqUpdate(Resource... rsc);
//    void markForReqUpdate(StorPool... storPool);
//    void markforReqUpdate(SnapshotDefinition... snapDfn);
//
//    void updateAppliedCtrl();
//    void updateApplied(Node... node);
//    void updateApplied(Resource... rsc);
//    void updateApplied(StorPool... storPool);
//    void updateApplied(SnapshotDefinition... snapDfn);
//
//    void needsFullSync();
//    void fullSyncApplied();
}
