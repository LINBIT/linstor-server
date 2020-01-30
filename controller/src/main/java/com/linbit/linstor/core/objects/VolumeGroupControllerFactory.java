package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class VolumeGroupControllerFactory
{
    private final VolumeGroupDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeGroupControllerFactory(
        VolumeGroupDatabaseDriver driverRef,
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

    public VolumeGroup create(
        AccessContext accCtx,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr,
        long initFlags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {

        rscGrp.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeGroup vlmGrpData = rscGrp.getVolumeGroup(accCtx, vlmNr);

        if (vlmGrpData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeGroup already exists");
        }

        vlmGrpData = new VolumeGroup(
            UUID.randomUUID(),
            rscGrp,
            vlmNr,
            initFlags,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.create(vlmGrpData);
        rscGrp.putVolumeGroup(accCtx, vlmGrpData);

        return vlmGrpData;
    }
}
