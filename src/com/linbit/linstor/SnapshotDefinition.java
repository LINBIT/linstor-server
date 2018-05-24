package com.linbit.linstor;

import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface SnapshotDefinition extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotDefinition>
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    SnapshotName getName();

    Snapshot getSnapshot(NodeName clNodeName);

    Collection<Snapshot> getAllSnapshots();

    void addSnapshot(Snapshot snapshotRef);

    void removeSnapshot(Snapshot snapshotRef);

    StateFlags<SnapshotDfnFlags> getFlags();

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

        public static SnapshotDfnFlags[] valuesOfIgnoreCase(String string)
        {
            SnapshotDfnFlags[] flags;
            if (string == null)
            {
                flags = new SnapshotDfnFlags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new SnapshotDfnFlags[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = SnapshotDfnFlags.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static SnapshotDfnFlags[] restoreFlags(long vlmDfnFlags)
        {
            List<SnapshotDfnFlags> flagList = new ArrayList<>();
            for (SnapshotDfnFlags flag : SnapshotDfnFlags.values())
            {
                if ((vlmDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new SnapshotDfnFlags[] {});
        }
    }

    public interface InitMaps
    {
    }
}
