package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Snapshot volume definitions are stored independently of the source volume definitions so that we have accurate
 * information about the content of the snapshots even when the source resource definition is later modified.
 */
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

    void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException;

    void removeSnapshotVolume(AccessContext accCtx, SnapshotVolumeData snapshotVolumeData)
        throws AccessDeniedException;

    long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, DatabaseException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<SnapshotVlmDfnFlags> getFlags();

    void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

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

    enum SnapshotVlmDfnFlags implements Flags
    {
        ENCRYPTED(1L);

        public final long flagValue;

        SnapshotVlmDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static SnapshotVlmDfnFlags[] restoreFlags(long snapshotVlmDfnFlags)
        {
            List<SnapshotVlmDfnFlags> flagList = new ArrayList<>();
            for (SnapshotVlmDfnFlags flag : SnapshotVlmDfnFlags.values())
            {
                if ((snapshotVlmDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new SnapshotVlmDfnFlags[0]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(SnapshotVlmDfnFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(SnapshotVlmDfnFlags.class, listFlags);
        }
    }

    public interface SnapshotVlmDfnApi
    {
        UUID getUuid();
        Integer getVolumeNr();
        long getSize();
        long getFlags();
    }

    public interface InitMaps
    {
        Map<NodeName, SnapshotVolume> getSnapshotVlmMap();
    }
}
