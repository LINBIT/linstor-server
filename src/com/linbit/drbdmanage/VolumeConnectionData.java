package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

/**
 * Defines a connection between two DRBD volumes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class VolumeConnectionData extends BaseTransactionObject implements VolumeConnection
{
    // Object identifier
    private final UUID objId;
    private final Volume sourceVolume;
    private final Volume targetVolume;

    private final Props props;

    private final VolumeConnectionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private VolumeConnectionData(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            sourceVolume,
            targetVolume,
            transMgr
        );
    }

    /*
     * used by dbDrivers and tests
     */
    VolumeConnectionData(
        UUID uuid,
        AccessContext accCtx,
        Volume sourceVolumeRef,
        Volume targetVolumeRef,
        TransactionMgr transMgr
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

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(
                sourceNodeName,
                targetNodeName,
                sourceVolumeRef.getResourceDefinition().getName(),
                sourceVolumeRef.getVolumeDefinition().getVolumeNumber()
            ),
            transMgr
        );

        dbDriver = DrbdManage.getVolumeConnectionDatabaseDriver();

        transObjs = Arrays.asList(
            sourceVolume,
            targetVolume,
            props
        );

        sourceVolume.setVolumeConnection(accCtx, this);
        targetVolume.setVolumeConnection(accCtx, this);
    }

    public static VolumeConnectionData getInstance(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, DrbdDataAlreadyExistsException
    {
        VolumeConnectionData volConData = null;

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
        source.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        target.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        VolumeConnectionDataDatabaseDriver dbDriver = DrbdManage.getVolumeConnectionDatabaseDriver();

        volConData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (failIfExists && volConData != null)
        {
            throw new DrbdDataAlreadyExistsException("The VolumeConnection already exists");
        }

        if (volConData == null && createIfNotExists)
        {
            volConData = new VolumeConnectionData(
                accCtx,
                source,
                target,
                transMgr
            );

            dbDriver.create(volConData, transMgr);
        }
        if (volConData != null)
        {
            volConData.initialized();
        }
        return volConData;
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
        checkDeleted();
        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        sourceVolume.removeVolumeConnection(accCtx, this);
        targetVolume.removeVolumeConnection(accCtx, this);

        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted VolumeConnection", null);
        }
    }
}
