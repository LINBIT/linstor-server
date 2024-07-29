package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Defines a connection between two LinStor volumes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class VolumeConnection extends AbsCoreObj<VolumeConnection>
{
    private final Volume sourceVolume;
    private final Volume targetVolume;

    private final Key vlmConKey;

    private final Props props;

    private final VolumeConnectionDatabaseDriver dbDriver;

    private VolumeConnection(
        UUID uuid,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        VolumeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(uuid, transObjFactory, transMgrProviderRef);


        dbDriver = dbDriverRef;
        sourceVolume = sourceVolumeRef;
        targetVolume = targetVolumeRef;

        NodeName sourceNodeName = sourceVolumeRef.getAbsResource().getNode().getName();
        NodeName targetNodeName = targetVolumeRef.getAbsResource().getNode().getName();
        vlmConKey = new Key(this);
        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                sourceNodeName,
                targetNodeName,
                sourceVolumeRef.getResourceDefinition().getName(),
                sourceVolumeRef.getVolumeDefinition().getVolumeNumber()
            ),
            toStringImpl(),
            LinStorObject.VLM_CONN
        );

        transObjs = Arrays.asList(
            sourceVolume,
            targetVolume,
            props,
            deleted
        );
    }

    public static VolumeConnection createWithSorting(
        UUID uuid,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        VolumeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        AccessContext accCtx
    ) throws DatabaseException, LinStorDataAlreadyExistsException, AccessDeniedException
    {
        VolumeConnection vol1ConData = sourceVolumeRef.getVolumeConnection(accCtx, targetVolumeRef);
        VolumeConnection vol2ConData = targetVolumeRef.getVolumeConnection(accCtx, sourceVolumeRef);

        if (vol1ConData != null || vol2ConData != null)
        {
            if (vol1ConData != null && vol2ConData != null)
            {
                throw new LinStorDataAlreadyExistsException("The VolumeConnection already exists");
            }
            throw new LinStorDataAlreadyExistsException(
                "The VolumeConnection already exists for one of the resources"
            );
        }

        if (
            sourceVolumeRef.getVolumeDefinition() != targetVolumeRef.getVolumeDefinition() ||
                sourceVolumeRef.getResourceDefinition() != targetVolumeRef.getResourceDefinition()
        )
        {
            throw new ImplementationError(
                String.format(
                    "Creating connection between unrelated Volumes %n" +
                        "Volume1: NodeName=%s, ResName=%s, VolNr=%d %n" +
                        "Volume2: NodeName=%s, ResName=%s, VolNr=%d.",
                    sourceVolumeRef.getAbsResource().getNode().getName().value,
                    sourceVolumeRef.getResourceDefinition().getName().value,
                    sourceVolumeRef.getVolumeDefinition().getVolumeNumber().value,
                    targetVolumeRef.getAbsResource().getNode().getName().value,
                    targetVolumeRef.getResourceDefinition().getName().value,
                    targetVolumeRef.getVolumeDefinition().getVolumeNumber().value
                ),
                null
            );
        }

        NodeName sourceNodeName = sourceVolumeRef.getAbsResource().getNode().getName();
        NodeName targetNodeName = targetVolumeRef.getAbsResource().getNode().getName();

        Volume src;
        Volume dst;
        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            src = sourceVolumeRef;
            dst = targetVolumeRef;
        }
        else
        {
            src = targetVolumeRef;
            dst = sourceVolumeRef;
        }

        return createForDb(
            uuid,
            src,
            dst,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef
        );
    }

    /**
     * WARNING: do not use this method unless you are absolutely sure the resourceConnection you are trying to create
     * does not exist yet and the resources are already sorted correctly.
     * If you are not sure they are, use VolumeConnection.createWithSorting instead.
     */
    public static VolumeConnection createForDb(
        UUID uuid,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        VolumeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    ) throws DatabaseException
    {
        return new VolumeConnection(
            uuid,
            sourceVolumeRef,
            targetVolumeRef,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef
        );
    }

    public static @Nullable VolumeConnection get(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws AccessDeniedException
    {
        Volume source;
        Volume target;

        NodeName sourceNodeName = sourceVolume.getAbsResource().getNode().getName();
        NodeName targetNodeName = targetVolume.getAbsResource().getNode().getName();

        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            source = sourceVolume;
            target = targetVolume;
        }
        else
        {
            source = targetVolume;
            target = sourceVolume;
        }

        return source.getVolumeConnection(accCtx, target);
    }

    public Volume getSourceVolume(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return sourceVolume;
    }

    public Volume getTargetVolume(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        targetVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return targetVolume;
    }

    public Key getKey()
    {
        // no check deleted
        return vlmConKey;
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            sourceVolume.getAbsResource().getObjProt(),
            targetVolume.getAbsResource().getObjProt(),
            props
        );
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            sourceVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            sourceVolume.removeVolumeConnection(accCtx, this);
            targetVolume.removeVolumeConnection(accCtx, this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public int compareTo(VolumeConnection other)
    {
        // since source and target are already sorted, it should be enough to only compare sourceVolumes with each other
        // and if those match then also the targetVolumes
        int cmp = sourceVolume.compareTo(other.sourceVolume);
        if (cmp == 0)
        {
            cmp = targetVolume.compareTo(other.targetVolume);
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(sourceVolume, targetVolume);
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
        else if (obj instanceof VolumeConnection)
        {
            VolumeConnection other = (VolumeConnection) obj;
            other.checkDeleted();
            ret = Objects.equals(sourceVolume, other.sourceVolume) && Objects.equals(targetVolume, other.targetVolume);
        }
        return ret;
    }

    @Override
    public String toStringImpl()
    {
        return "Node1: '" + vlmConKey.sourceNodeName + "', " +
            "Node2: '" + vlmConKey.targetNodeName + "', " +
            "Rsc: '" + vlmConKey.rscName + "', " +
            "VlmNr: '" + vlmConKey.vlmNr + "'";
    }

    /**
     * Identifies a volumeConnection.
     */
    public static class Key implements Comparable<Key>
    {
        private final NodeName sourceNodeName;
        private final NodeName targetNodeName;
        private final ResourceName rscName;
        private final VolumeNumber vlmNr;

        public Key(VolumeConnection vlmConn)
        {
            this(
                vlmConn.sourceVolume.getAbsResource().getNode().getName(),
                vlmConn.targetVolume.getAbsResource().getNode().getName(),
                vlmConn.sourceVolume.getResourceDefinition().getName(),
                vlmConn.sourceVolume.getVolumeNumber()
            );
        }

        public Key(
            NodeName sourceNodeNameRef,
            NodeName targetNodeNameRef,
            ResourceName rscNameRef,
            VolumeNumber vlmNrRef
        )
        {
            sourceNodeName = sourceNodeNameRef;
            targetNodeName = targetNodeNameRef;
            rscName = rscNameRef;
            vlmNr = vlmNrRef;
        }

        public NodeName getSourceNodeName()
        {
            return sourceNodeName;
        }

        public NodeName getTargetNodeName()
        {
            return targetNodeName;
        }

        public ResourceName getRscName()
        {
            return rscName;
        }

        public VolumeNumber getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, sourceNodeName, targetNodeName, vlmNr);
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
            return Objects.equals(rscName, other.rscName) && Objects.equals(sourceNodeName, other.sourceNodeName) &&
                Objects.equals(targetNodeName, other.targetNodeName) && Objects.equals(vlmNr, other.vlmNr);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = rscName.compareTo(other.rscName);
            if (eq == 0)
            {
                eq = vlmNr.compareTo(other.vlmNr);
                if (eq == 0)
                {
                    eq = sourceNodeName.compareTo(other.sourceNodeName);
                    if (eq == 0)
                    {
                        eq = targetNodeName.compareTo(other.targetNodeName);
                    }
                }
            }
            return eq;
        }
    }
}
