package com.linbit.linstor.core.objects;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class SnapshotVolumeDefinitionControllerFactory
{
    private final SnapshotVolumeDefinitionDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDefinitionControllerFactory(
        SnapshotVolumeDefinitionDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotVolumeDefinition create(
        AccessContext accCtx,
        SnapshotDefinition snapshotDfn,
        VolumeDefinition vlmDfn,
        long volSize,
        SnapshotVolumeDefinition.Flags[] initFlags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotVolumeDefinition snapshotVolumeDefinition =
            snapshotDfn.getSnapshotVolumeDefinition(accCtx, vlmDfn.getVolumeNumber());

        if (snapshotVolumeDefinition != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolumeDefinition already exists");
        }

        snapshotVolumeDefinition = new SnapshotVolumeDefinition(
            UUID.randomUUID(),
            snapshotDfn,
            vlmDfn,
            vlmDfn.getVolumeNumber(),
            volSize,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );

        driver.create(snapshotVolumeDefinition);
        snapshotDfn.addSnapshotVolumeDefinition(accCtx, snapshotVolumeDefinition);

        return snapshotVolumeDefinition;
    }
}
