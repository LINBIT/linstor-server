package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Map;
import java.util.UUID;

public interface SnapshotVolumeDefinition
    extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotVolumeDefinition>
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    VolumeNumber getVolumeNumber();

    void addSnapshotVolume(SnapshotVolume snapshotVolume);

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
    }

    public interface InitMaps
    {
        Map<NodeName, SnapshotVolume> getSnapshotVlmMap();
    }
}
