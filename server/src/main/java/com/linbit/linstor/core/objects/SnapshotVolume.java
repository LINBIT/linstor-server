package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Objects;
import java.util.UUID;

/**
 * Snapshot volumes are stored independently of the source volumes so that we have accurate information about the
 * content of the snapshots even when the source resource is later modified or deleted.
 */
public interface SnapshotVolume extends TransactionObject, DbgInstanceUuid, Comparable<SnapshotVolume>
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
        throws AccessDeniedException, DatabaseException;

    SnapshotVlmApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    default Key getKey()
    {
        // no access or deleted check
        return new Key(getResourceName(), getVolumeNumber(), getSnapshotName());
    }

    interface SnapshotVlmApi
    {
        UUID getSnapshotVlmUuid();
        UUID getSnapshotVlmDfnUuid();
        String getStorPoolName();
        UUID getStorPoolUuid();
        int getSnapshotVlmNr();
    }

    class Key
    {
        private final String rscName;
        private final int vlmNr;
        private final String snapName;

        public Key(ResourceName rscNameRef, VolumeNumber vlmNrRef, SnapshotName snapNameRef)
        {
            rscName = rscNameRef.displayValue;
            vlmNr = vlmNrRef.value;
            snapName = snapNameRef.displayValue;
        }

        public Key(String rscNameRef, int vlmNrRef, String snapNameRef)
        {
            Objects.requireNonNull(rscNameRef);
            Objects.requireNonNull(snapNameRef);
            rscName = rscNameRef;
            vlmNr = vlmNrRef;
            snapName = snapNameRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((rscName == null) ? 0 : rscName.toUpperCase().hashCode());
            result = prime * result + ((snapName == null) ? 0 : snapName.toUpperCase().hashCode());
            result = prime * result + vlmNr;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = obj != null && obj instanceof Key;
            if (eq)
            {
                Key other = (Key) obj;
                eq &=
                    rscName.equalsIgnoreCase(other.rscName) &&
                    snapName.equalsIgnoreCase(other.snapName) &&
                    vlmNr == other.vlmNr;
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "SnapshotVolume.Key [rscName=" + rscName + ", vlmNr=" + vlmNr + ", snapName=" + snapName + "]";
        }
    }
}
