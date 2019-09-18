package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class SnapshotVolumeDefinitionData extends BaseTransactionObject implements SnapshotVolumeDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    // Net volume size in kiB
    private final TransactionSimpleObject<SnapshotVolumeDefinition, Long> volumeSize;

    private final SnapshotVolumeDefinitionDatabaseDriver dbDriver;

    // Properties container for this snapshot volume definition
    private final Props snapshotVlmDfnProps;

    // State flags
    private final StateFlags<SnapshotVlmDfnFlags> flags;

    private final TransactionMap<NodeName, SnapshotVolume> snapshotVlmMap;

    private final TransactionSimpleObject<SnapshotVolumeDefinitionData, Boolean> deleted;

    public SnapshotVolumeDefinitionData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        VolumeNumber volNr,
        long volSize,
        long initFlags,
        SnapshotVolumeDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<NodeName, SnapshotVolume> snapshotVlmMapRef
    )
        throws MdException, DatabaseException
    {
        super(transMgrProviderRef);
        VolumeDefinitionData.checkVolumeSize(volSize);

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        volumeNr = volNr;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotVlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(snapshotDfnRef.getResourceName(), snapshotDfnRef.getName(), volumeNr)
        );

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfnRef.getResourceDefinition().getObjProt(),
            this,
            SnapshotVlmDfnFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVlmMap = transObjFactory.createTransactionMap(snapshotVlmMapRef, null);

        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            snapshotDfn,
            snapshotVlmMap,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return volumeNr;
    }

    @Override
    public void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVlmMap.put(snapshotVolume.getNodeName(), snapshotVolume);
    }

    @Override
    public void removeSnapshotVolume(
        AccessContext accCtx,
        SnapshotVolume snapshotVolume
    )
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVlmMap.remove(snapshotVolume.getNodeName());
    }

    @Override
    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    @Override
    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return volumeSize.set(newVolumeSize);
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, getResourceDefinition().getObjProt(), snapshotVlmDfnProps);
    }

    @Override
    public StateFlags<SnapshotVlmDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws DatabaseException, AccessDeniedException
    {
        if (!deleted.get())
        {
            getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshotDfn.removeSnapshotVolumeDefinition(accCtx, volumeNr);

            snapshotVlmDfnProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot volume definition");
        }
    }

    @Override
    public String toString()
    {
        return snapshotDfn + ", VlmNr: '" + volumeNr + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public SnapshotVlmDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getVolumeSize(accCtx),
            flags.getFlagsBits(accCtx)
        );
    }
}
