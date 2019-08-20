package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.UUID;

/**
 * Defines a connection between two LinStor volumes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class VolumeConnectionData extends BaseTransactionObject implements VolumeConnection
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Volume sourceVolume;
    private final Volume targetVolume;

    private final Props props;

    private final VolumeConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeConnectionData, Boolean> deleted;

    VolumeConnectionData(
        UUID uuid,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        VolumeConnectionDataDatabaseDriver dbDriverRef,
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
                        sourceVolumeRef.getResource().getAssignedNode().getName().value,
                        sourceVolumeRef.getResourceDefinition().getName().value,
                        sourceVolumeRef.getVolumeDefinition().getVolumeNumber().value,
                        targetVolumeRef.getResource().getAssignedNode().getName().value,
                        targetVolumeRef.getResourceDefinition().getName().value,
                        targetVolumeRef.getVolumeDefinition().getVolumeNumber().value
                    ),
                null
            );
        }

        objId = uuid;
        dbDriver = dbDriverRef;
        dbgInstanceId = UUID.randomUUID();

        NodeName sourceNodeName = sourceVolumeRef.getResource().getAssignedNode().getName();
        NodeName targetNodeName = targetVolumeRef.getResource().getAssignedNode().getName();

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

    public static VolumeConnectionData get(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws AccessDeniedException
    {
        Volume source;
        Volume target;

        NodeName sourceNodeName = sourceVolume.getResource().getAssignedNode().getName();
        NodeName targetNodeName = targetVolume.getResource().getAssignedNode().getName();

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

        return (VolumeConnectionData) source.getVolumeConnection(accCtx, target);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Volume getSourceVolume(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return sourceVolume;
    }

    @Override
    public Volume getTargetVolume(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return targetVolume;
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            sourceVolume.getResource().getObjProt(),
            targetVolume.getResource().getObjProt(),
            props
        );
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

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
    public String toString()
    {
        return "Node1: '" + sourceVolume.getResource().getAssignedNode().getName() + "', " +
               "Node2: '" + targetVolume.getResource().getAssignedNode().getName() + "', " +
               "Rsc: '" + sourceVolume.getResourceDefinition().getName() + "', " +
               "VlmNr: '" + sourceVolume.getVolumeDefinition().getVolumeNumber() + "'";
    }
}
