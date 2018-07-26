package com.linbit.linstor;

import com.linbit.drbd.md.MdException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
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
        VolumeNumber volumeNumber,
        long volSize,
        SnapshotVolumeDefinition.SnapshotVlmDfnFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotVolumeDefinition snapshotVolumeDefinition =
            snapshotDfn.getSnapshotVolumeDefinition(accCtx, volumeNumber);

        if (snapshotVolumeDefinition != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolumeDefinition already exists");
        }

        snapshotVolumeDefinition = new SnapshotVolumeDefinitionData(
            UUID.randomUUID(),
            snapshotDfn,
            volumeNumber,
            volSize,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        driver.create(snapshotVolumeDefinition);
        snapshotDfn.addSnapshotVolumeDefinition(accCtx, snapshotVolumeDefinition);

        return snapshotVolumeDefinition;
    }
}
