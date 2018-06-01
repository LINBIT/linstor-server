package com.linbit.linstor;

import com.google.protobuf.MapFieldLite;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface SnapshotDefinition extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotDefinition>
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    SnapshotName getName();

    SnapshotVolumeDefinition getSnapshotVolumeDefinition(VolumeNumber volumeNumber);

    Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions();

    void addSnapshotVolumeDefinition(SnapshotVolumeDefinition snapshotVolumeDefinition);

    void removeSnapshotVolumeDefinition(VolumeNumber volumeNumber);

    Snapshot getSnapshot(NodeName clNodeName);

    Collection<Snapshot> getAllSnapshots();

    void addSnapshot(Snapshot snapshotRef);

    void removeSnapshot(Snapshot snapshotRef);

    StateFlags<SnapshotDfnFlags> getFlags();

    SnapshotDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    @Override
    default int compareTo(SnapshotDefinition otherSnapshotDfn)
    {
        int eq = getResourceDefinition().compareTo(otherSnapshotDfn.getResourceDefinition());
        if (eq == 0)
        {
            eq = getName().compareTo(otherSnapshotDfn.getName());
        }
        return eq;
    }

    enum SnapshotDfnFlags implements Flags
    {
        SUCCESSFUL(1L),
        FAILED_DEPLOYMENT(2L),
        FAILED_DISCONNECT(4L);

        public final long flagValue;

        SnapshotDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static SnapshotDfnFlags[] restoreFlags(long snapshotDfnFlags)
        {
            List<SnapshotDfnFlags> flagList = new ArrayList<>();
            for (SnapshotDfnFlags flag : SnapshotDfnFlags.values())
            {
                if ((snapshotDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new SnapshotDfnFlags[0]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(SnapshotDfnFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(SnapshotDfnFlags.class, listFlags);
        }
    }

    public interface SnapshotDfnApi
    {
        ResourceDefinition.RscDfnApi getRscDfn();
        UUID getUuid();
        String getSnapshotName();
        long getFlags();
        List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> getSnapshotVlmDfnList();
    }

    public interface InitMaps
    {
        Map<NodeName, Snapshot> getSnapshotMap();
        Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap();
    }
}
