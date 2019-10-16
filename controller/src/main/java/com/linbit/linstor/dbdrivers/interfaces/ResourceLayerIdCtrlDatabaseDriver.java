package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;

public interface ResourceLayerIdCtrlDatabaseDriver extends ResourceLayerIdDatabaseDriver
{
    public static class RscLayerInfo
    {
        public final NodeName nodeName;
        public final ResourceName resourceName;
        public final SnapshotName snapshotName;
        public final int id;
        public final Integer parentId;
        public final DeviceLayerKind kind;
        public final String rscSuffix;

        public RscLayerInfo(
            NodeName nodeNameRef,
            ResourceName resourceNameRef,
            SnapshotName snapshotNameRef,
            int idRef,
            Integer parentIdRef,
            DeviceLayerKind kindRef,
            String rscSuffixRef
        )
        {
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            id = idRef;
            parentId = parentIdRef;
            kind = kindRef;
            rscSuffix = rscSuffixRef;
        }
    }

    /*
     * Methods only needed for loading
     */
    List<? extends RscLayerInfo> loadAllResourceIds() throws DatabaseException;
}
