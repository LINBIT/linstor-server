package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class SnapshotVolume extends BaseTransactionObject implements DbgInstanceUuid, Comparable<SnapshotVolume>
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Snapshot snapshot;

    private final SnapshotVolumeDefinition snapshotVolumeDefinition;

    private final StorPool storPool;

    private final SnapshotVolumeDatabaseDriver dbDriver;

    private final TransactionSimpleObject<SnapshotVolume, Boolean> deleted;

    public SnapshotVolume(
        UUID objIdRef,
        Snapshot snapshotRef,
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        StorPool storPoolRef,
        SnapshotVolumeDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshot = snapshotRef;
        snapshotVolumeDefinition = snapshotVolumeDefinitionRef;
        storPool = storPoolRef;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            snapshot,
            snapshotVolumeDefinition,
            deleted
        );
    }

    public UUID getUuid()
    {
        return objId;
    }

    public Snapshot getSnapshot()
    {
        checkDeleted();
        return snapshot;
    }

    public SnapshotVolumeDefinition getSnapshotVolumeDefinition()
    {
        checkDeleted();
        return snapshotVolumeDefinition;
    }

    public StorPool getStorPool(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPool;
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            getResourceDefinition()
                .getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshot.removeSnapshotVolume(accCtx, this);
            snapshotVolumeDefinition.removeSnapshotVolume(accCtx, this);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot volume");
        }
    }

    @Override
    public String toString()
    {
        return snapshot + ", VlmNr: '" + getVolumeNumber() + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public int compareTo(SnapshotVolume other)
    {
        int cmp = snapshot.compareTo(other.getSnapshot());
        if (cmp == 0)
        {
            cmp = snapshotVolumeDefinition.compareTo(other.getSnapshotVolumeDefinition());
        }
        return cmp;
    }

    public SnapshotVolumeApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmPojo(
            getStorPool(accCtx).getName().getDisplayName(),
            getStorPool(accCtx).getUuid(),
            getSnapshotVolumeDefinition().getUuid(),
            getUuid(),
            getVolumeNumber().value
        );
    }

    public Node getNode()
    {
        return getSnapshot().getNode();
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        return getSnapshot().getSnapshotDefinition();
    }

    public ResourceDefinition getResourceDefinition()
    {
        return getSnapshotDefinition().getResourceDefinition();
    }

    public ResourceName getResourceName()
    {
        return getResourceDefinition().getName();
    }

    public SnapshotName getSnapshotName()
    {
        return getSnapshotDefinition().getName();
    }

    public NodeName getNodeName()
    {
        return getNode().getName();
    }

    public VolumeNumber getVolumeNumber()
    {
        return getSnapshotVolumeDefinition().getVolumeNumber();
    }

    public Key getKey()
    {
        // no access or deleted check
        return new Key(getResourceName(), getVolumeNumber(), getSnapshotName());
    }

    public class Key
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
                eq &= rscName.equalsIgnoreCase(other.rscName) &&
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
