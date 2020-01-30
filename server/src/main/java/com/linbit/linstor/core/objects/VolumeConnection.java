package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.UUID;

/**
 * Defines a connection between two LinStor volumes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class VolumeConnection extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<VolumeConnection>
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Volume sourceVolume;
    private final Volume targetVolume;

    private final Props props;

    private final VolumeConnectionDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeConnection, Boolean> deleted;

    VolumeConnection(
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
        super(transMgrProviderRef);

        if (sourceVolumeRef.getVolumeDefinition() != targetVolumeRef.getVolumeDefinition() ||
            sourceVolumeRef.getResourceDefinition() != targetVolumeRef.getResourceDefinition())
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

        objId = uuid;
        dbDriver = dbDriverRef;
        dbgInstanceId = UUID.randomUUID();

        NodeName sourceNodeName = sourceVolumeRef.getAbsResource().getNode().getName();
        NodeName targetNodeName = targetVolumeRef.getAbsResource().getNode().getName();

        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            sourceVolume = sourceVolumeRef;
            targetVolume = targetVolumeRef;
        }
        else
        {
            sourceVolume = targetVolumeRef;
            targetVolume = sourceVolumeRef;
        }

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                sourceNodeName,
                targetNodeName,
                sourceVolumeRef.getResourceDefinition().getName(),
                sourceVolumeRef.getVolumeDefinition().getVolumeNumber()
            )
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            sourceVolume,
            targetVolume,
            props,
            deleted
        );
    }

    public static VolumeConnection get(
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

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
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

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume connection");
        }
    }

    @Override
    public int compareTo(VolumeConnection other)
    {
        return (sourceVolume.getAbsResource().getNode().getName().value +
            targetVolume.getAbsResource().getNode().getName().value +
            sourceVolume.getResourceDefinition().getName().value +
            sourceVolume.getVolumeDefinition().getVolumeNumber()).compareTo(
                other.sourceVolume.getAbsResource().getNode().getName().value +
                other.targetVolume.getAbsResource().getNode().getName().value +
                other.sourceVolume.getResourceDefinition().getName().value +
                other.sourceVolume.getVolumeDefinition().getVolumeNumber()
        );
    }

    @Override
    public String toString()
    {
        return "Node1: '" + sourceVolume.getAbsResource().getNode().getName() + "', " +
               "Node2: '" + targetVolume.getAbsResource().getNode().getName() + "', " +
               "Rsc: '" + sourceVolume.getResourceDefinition().getName() + "', " +
               "VlmNr: '" + sourceVolume.getVolumeDefinition().getVolumeNumber() + "'";
    }
}
