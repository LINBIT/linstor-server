package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class SnapshotVolume extends AbsVolume<Snapshot> // TODO implement SnapshotVolumeConnections
{
    private static final String TO_STRING_FORMAT = "Node: '%s', Resource: '%s', Snapshot: '%s', VlmNr: '%s'";
    private final SnapshotVolumeDefinition snapshotVolumeDefinition;

    private final SnapshotVolumeDatabaseDriver dbDriver;

    private final Props snapVlmProps;
    private final ReadOnlyProps vlmRoProps;
    private final Props vlmProps;

    // deliberately not a TransactionObject to behave the same as SatelliteResourceStates
    private volatile String state;

    private final Key snapVlmKey;

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
            transObjFactory,
            transMgrProviderRef
        );

        snapshotVolumeDefinition = snapshotVolumeDefinitionRef;
        dbDriver = dbDriverRef;
        snapVlmKey = new Key(this);

        snapVlmProps = propsConFactory.getInstance(
            PropsContainer.buildPath(
                snapshotRef.getNodeName(),
                snapshotRef.getResourceName(),
                snapshotRef.getSnapshotName(),
                snapshotVolumeDefinitionRef.getVolumeNumber(),
                false
            ),
            toStringImpl(),
            LinStorObject.SNAP_VLM
        );

        vlmProps = propsConFactory.getInstance(
            PropsContainer.buildPath(
                snapshotRef.getNodeName(),
                snapshotRef.getResourceName(),
                snapshotRef.getSnapshotName(),
                snapshotVolumeDefinitionRef.getVolumeNumber(),
                false
            ),
            toStringImpl(),
            LinStorObject.VLM
        );
        vlmRoProps = new ReadOnlyPropsImpl(vlmProps);

        transObjs.addAll(
            Arrays.asList(
                snapshotVolumeDefinition,
                deleted,
                snapVlmProps,
                vlmProps
            )
        );
    }

    public SnapshotVolumeDefinition getSnapshotVolumeDefinition()
    {
        checkDeleted();
        return snapshotVolumeDefinition;
    }

    public String getState(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return state;
    }

    public void setState(AccessContext accCtx, String stateRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        state = stateRef;
    }

    public Props getSnapVlmProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, getObjProt(), snapVlmProps);
    }

    public ReadOnlyProps getVlmProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return vlmRoProps;
    }

    public Props getVlmPropsForChange(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return vlmProps;
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

            snapVlmProps.delete();
            vlmProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public String toStringImpl()
    {
        return String.format(
            TO_STRING_FORMAT,
            snapVlmKey.nodeName,
            snapVlmKey.rscName,
            snapVlmKey.snapName,
            snapVlmKey.vlmNr
        );
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

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(snapshotVolumeDefinition, absRsc);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof SnapshotVolume)
        {
            SnapshotVolume other = (SnapshotVolume) obj;
            other.checkDeleted();
            ret = Objects.equals(snapshotVolumeDefinition, other.snapshotVolumeDefinition) &&
                Objects.equals(absRsc, other.absRsc);
        }
        return ret;
    }

    public SnapshotVolumeApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return new SnapshotVlmPojo(
            getSnapshotVolumeDefinition().getUuid(),
            getUuid(),
            getVolumeNumber().value,
            snapVlmProps.map(),
            vlmRoProps.map(),
            state
        );
    }

    public Node getNode()
    {
        checkDeleted();
        return absRsc.getNode();
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return absRsc.getSnapshotDefinition();
    }

    public Snapshot getSnapshot()
    {
        checkDeleted();
        return absRsc;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
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
        checkDeleted();
        return getSnapshotVolumeDefinition().getVolumeSize(dbCtxRef);
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return getSnapshotVolumeDefinition().getVolumeDefinition();
    }

    public Key getKey()
    {
        // no access or deleted check
        return snapVlmKey;
    }

    public class Key
    {
        private final ResourceName rscName;
        private final VolumeNumber vlmNr;
        private final SnapshotName snapName;
        private final NodeName nodeName;

        public Key(SnapshotVolume snapVlm)
        {
            this(
                snapVlm.absRsc.getResourceName(),
                snapVlm.getVolumeNumber(),
                snapVlm.absRsc.getSnapshotName(),
                snapVlm.absRsc.getNodeName()
            );
        }

        public Key(ResourceName rscNameRef, VolumeNumber vlmNrRef, SnapshotName snapNameRef, NodeName nodeNameRef)
        {
            rscName = rscNameRef;
            vlmNr = vlmNrRef;
            snapName = snapNameRef;
            nodeName = nodeNameRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + Objects.hash(nodeName, rscName, snapName, vlmNr);
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key))
            {
                return false;
            }
            Key other = (Key) obj;
            if (!getEnclosingInstance().equals(other.getEnclosingInstance()))
            {
                return false;
            }
            return Objects.equals(nodeName, other.nodeName) && Objects.equals(rscName, other.rscName) && Objects.equals(
                snapName,
                other.snapName
            ) && Objects.equals(vlmNr, other.vlmNr);
        }

        @Override
        public String toString()
        {
            return "SnapshotVolume.Key [nodeName=" + nodeName + ", rscName=" + rscName + ", vlmNr=" + vlmNr +
                ", snapName=" + snapName + "]";
        }

        private SnapshotVolume getEnclosingInstance()
        {
            return SnapshotVolume.this;
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

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return getResourceDefinition().getObjProt();
    }
}
