package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface Snapshot extends TransactionObject, DbgInstanceUuid, Comparable<Snapshot>
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    Node getNode();

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

    void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException;

    SnapshotVolume getSnapshotVolume(AccessContext accCtx, VolumeNumber volumeNumber)
        throws AccessDeniedException;

    Collection<SnapshotVolume> getAllSnapshotVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    void removeSnapshotVolume(AccessContext accCtx, SnapshotVolumeData snapshotVolumeData)
        throws AccessDeniedException;

    StateFlags<SnapshotFlags> getFlags();

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    boolean getSuspendResource(AccessContext accCtx)
        throws AccessDeniedException;

    void setSuspendResource(AccessContext accCtx, boolean suspendResource)
        throws AccessDeniedException;

    boolean getTakeSnapshot(AccessContext accCtx)
        throws AccessDeniedException;

    void setTakeSnapshot(AccessContext accCtx, boolean takeSnapshot)
        throws AccessDeniedException;

    SnapshotApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    @Override
    default int compareTo(Snapshot otherSnapshot)
    {
        int eq = getSnapshotDefinition().compareTo(
            otherSnapshot.getSnapshotDefinition()
        );
        if (eq == 0)
        {
            eq = getNode().compareTo(otherSnapshot.getNode());
        }
        return eq;
    }

    enum SnapshotFlags implements Flags
    {
        DELETE(1L),
        ROLLBACK_TARGET(2L);

        public final long flagValue;

        SnapshotFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static SnapshotFlags[] restoreFlags(long snapshotFlags)
        {
            List<SnapshotFlags> flagList = new ArrayList<>();
            for (SnapshotFlags flag : SnapshotFlags.values())
            {
                if ((snapshotFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new SnapshotFlags[0]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(SnapshotFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(SnapshotFlags.class, listFlags);
        }
    }

    public interface SnapshotApi
    {
        SnapshotDefinition.SnapshotDfnApi getSnaphotDfn();
        UUID getSnapshotUuid();
        long getFlags();
        boolean getSuspendResource();
        boolean getTakeSnapshot();
        Long getFullSyncId();
        Long getUpdateId();
        List<? extends SnapshotVolume.SnapshotVlmApi> getSnapshotVlmList();
    }

    public interface InitMaps
    {
        Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap();
    }
}
