package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class SnapshotVolume extends AbsVolume<Snapshot> // TODO implement SnapshotVolumeConnections
{
    private final SnapshotVolumeDefinition snapshotVolumeDefinition;

    private final SnapshotVolumeDatabaseDriver dbDriver;

    public SnapshotVolume(
        UUID objIdRef,
        Snapshot snapshotRef,
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        SnapshotVolumeDatabaseDriver dbDriverRef,
        PropsContainerFactory propsConFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(
            objIdRef,
            snapshotRef,
            propsConFactory.getInstance(
                PropsContainer.buildPath(
                    snapshotRef.getResourceName(),
                    snapshotRef.getSnapshotName(),
                    snapshotVolumeDefinitionRef.getVolumeNumber()
                )
            ),
            transObjFactory,
            transMgrProviderRef
        );

        snapshotVolumeDefinition = snapshotVolumeDefinitionRef;
        dbDriver = dbDriverRef;

        transObjs.addAll(
            Arrays.asList(
                snapshotVolumeDefinition,
                deleted
            )
        );
    }

    public SnapshotVolumeDefinition getSnapshotVolumeDefinition()
    {
        checkDeleted();
        return snapshotVolumeDefinition;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            absRsc.removeVolume(accCtx, this);
            snapshotVolumeDefinition.removeSnapshotVolume(accCtx, this);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public String toString()
    {
        return absRsc + ", VlmNr: '" + getVolumeNumber() + "'";
    }

    @Override
    public int compareTo(AbsVolume<Snapshot> other)
    {
        int cmp = -1;
        if (other instanceof SnapshotVolume)
        {
            cmp = absRsc.compareTo(other.getAbsResource());
            if (cmp == 0)
            {
                cmp = snapshotVolumeDefinition.compareTo(((SnapshotVolume) other).getSnapshotVolumeDefinition());
            }
        }
        return cmp;
    }

    public SnapshotVolumeApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmPojo(
            getSnapshotVolumeDefinition().getUuid(),
            getUuid(),
            getVolumeNumber().value
        );
    }

    public Node getNode()
    {
        return absRsc.getNode();
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        return absRsc.getSnapshotDefinition();
    }

    public Snapshot getSnapshot()
    {
        return absRsc;
    }

    @Override
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

    @Override
    public VolumeNumber getVolumeNumber()
    {
        return getSnapshotVolumeDefinition().getVolumeNumber();
    }

    @Override
    public long getVolumeSize(AccessContext dbCtxRef) throws AccessDeniedException
    {
        return getSnapshotVolumeDefinition().getVolumeSize(dbCtxRef);
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        return getSnapshotVolumeDefinition().getVolumeDefinition();
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

    @Override
    public ApiCallRc getReports()
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void addReports(ApiCallRc apiCallRcRef)
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public void clearReports()
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    public Stream<TransactionObject> streamSnapshotVolumeConnections(AccessContext accCtxRef)
        throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }

    public TransactionObject getSnapshotVolumeConnection(AccessContext accCtxRef, SnapshotVolume othervolumeRef)
        throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }

    public void setSnapshotVolumeConnection(AccessContext accCtxRef, TransactionObject volumeConnectionRef)
        throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }

    public void removeSnapshotVolumeConnection(AccessContext accCtxRef, TransactionObject volumeConnectionRef)
        throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }
}
