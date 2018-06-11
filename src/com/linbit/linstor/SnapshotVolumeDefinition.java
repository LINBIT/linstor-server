package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface SnapshotVolumeDefinition
    extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotVolumeDefinition>
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    VolumeNumber getVolumeNumber();

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

    void addSnapshotVolume(SnapshotVolume snapshotVolume);

    void removeSnapshotVolume(SnapshotVolumeData snapshotVolumeData);

    long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    SnapshotVlmDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException;

    @Override
    default int compareTo(SnapshotVolumeDefinition otherSnapshotVlmDfn)
    {
        int eq = getSnapshotDefinition().compareTo(
            otherSnapshotVlmDfn.getSnapshotDefinition()
        );
        if (eq == 0)
        {
            eq = getVolumeNumber().compareTo(otherSnapshotVlmDfn.getVolumeNumber());
        }
        return eq;
    }

    public interface SnapshotVlmDfnApi
    {
        UUID getUuid();
        Integer getVolumeNr();
        long getSize();
    }

    public interface InitMaps
    {
        Map<NodeName, SnapshotVolume> getSnapshotVlmMap();
    }
}
