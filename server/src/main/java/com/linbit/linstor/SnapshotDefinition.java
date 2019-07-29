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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public interface SnapshotDefinition extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotDefinition>
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    SnapshotName getName();

    default ResourceName getResourceName()
    {
        return getResourceDefinition().getName();
    }

    SnapshotVolumeDefinition getSnapshotVolumeDefinition(
        AccessContext accCtx,
        VolumeNumber volumeNumber
    )
        throws AccessDeniedException;

    Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions(AccessContext accCtx)
        throws AccessDeniedException;

    void addSnapshotVolumeDefinition(
        AccessContext accCtx,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
        throws AccessDeniedException;

    void removeSnapshotVolumeDefinition(AccessContext accCtx, VolumeNumber volumeNumber)
        throws AccessDeniedException;

    Snapshot getSnapshot(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    Collection<Snapshot> getAllSnapshots(AccessContext accCtx)
        throws AccessDeniedException;

    void addSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException;

    void removeSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<SnapshotDfnFlags> getFlags();

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    boolean isDeleted();

    /**
     * Is the snapshot being used for a linstor action such as creation, deletion or rollback?
     * @param accCtx
     */
    boolean getInProgress(AccessContext accCtx)
        throws AccessDeniedException;

    void setInCreation(AccessContext accCtx, boolean inCreationRef)
        throws DatabaseException, AccessDeniedException;

    SnapshotDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    SnapshotDfnListItemApi getListItemApiData(AccessContext accCtx) throws AccessDeniedException;

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
        FAILED_DISCONNECT(4L),
        DELETE(8L);

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
        Map<String, String> getProps();
        List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> getSnapshotVlmDfnList();
    }

    public interface SnapshotDfnListItemApi extends SnapshotDfnApi
    {
        List<String> getNodeNames();
    }

    /**
     * Identifies a snapshot within a node.
     */
    class Key implements Comparable<Key>
    {
        private final ResourceName resourceName;

        private final SnapshotName snapshotName;

        public Key(SnapshotDefinition snapshotDefinition)
        {
            this(snapshotDefinition.getResourceName(), snapshotDefinition.getName());
        }

        public Key(ResourceName resourceNameRef, SnapshotName snapshotNameRef)
        {
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public SnapshotName getSnapshotName()
        {
            return snapshotName;
        }

        @Override
        // Code style exception: Automatically generated code
        @SuppressWarnings({"DescendantToken", "ParameterName"})
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(snapshotName, that.snapshotName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resourceName, snapshotName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(Key other)
        {
            int eq = resourceName.compareTo(other.resourceName);
            if (eq == 0)
            {
                eq = snapshotName.compareTo(other.snapshotName);
            }
            return eq;
        }
    }

    public interface InitMaps
    {
        Map<NodeName, Snapshot> getSnapshotMap();
        Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap();
    }
}
