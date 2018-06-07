package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface SnapshotVolume extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    Snapshot getSnapshot();

    SnapshotVolumeDefinition getSnapshotVolumeDefinition();

    default Node getNode()
    {
        return getSnapshot().getNode();
    }

    default SnapshotDefinition getSnapshotDefinition()
    {
        return getSnapshot().getSnapshotDefinition();
    }

    default ResourceDefinition getResourceDefinition()
    {
        return getSnapshotDefinition().getResourceDefinition();
    }

    default ResourceName getResourceName()
    {
        return getResourceDefinition().getName();
    }

    default SnapshotName getSnapshotName()
    {
        return getSnapshotDefinition().getName();
    }

    default NodeName getNodeName()
    {
        return getNode().getName();
    }

    default VolumeNumber getVolumeNumber()
    {
        return getSnapshotVolumeDefinition().getVolumeNumber();
    }

    StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    SnapshotVlmApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    interface SnapshotVlmApi
    {
        UUID getSnapshotVlmUuid();
        UUID getSnapshotVlmDfnUuid();
        String getStorPoolName();
        UUID getStorPoolUuid();
        int getSnapshotVlmNr();
    }
}
