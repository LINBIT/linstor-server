package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import java.sql.SQLException;
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
        AccessContext accCtx,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        TransactionMgr transMgr,
        VolumeConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory
    )
        throws SQLException, AccessDeniedException
    {
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
            ),
            transMgr
        );
        deleted = new TransactionSimpleObject<>(this, false, null);

        transObjs = Arrays.asList(
            sourceVolume,
            targetVolume,
            props,
            deleted
        );

        sourceVolume.setVolumeConnection(accCtx, this);
        targetVolume.setVolumeConnection(accCtx, this);
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
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            sourceVolume.removeVolumeConnection(accCtx, this);
            targetVolume.removeVolumeConnection(accCtx, this);

            props.delete();

            dbDriver.delete(this, transMgr);

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
