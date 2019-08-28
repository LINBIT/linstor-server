package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class VolumeGroupDataControllerFactory
{
    private final VolumeGroupDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeGroupDataControllerFactory(
        VolumeGroupDataDatabaseDriver driverRef,
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

    public VolumeGroupData create(
        AccessContext accCtx,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {

        rscGrp.getObjProt().requireAccess(accCtx, AccessType.USE);

        VolumeGroupData vlmGrpData = (VolumeGroupData) rscGrp.getVolumeGroup(accCtx, vlmNr);

        if (vlmGrpData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeGroup already exists");
        }

        vlmGrpData = new VolumeGroupData(
            UUID.randomUUID(),
            rscGrp,
            vlmNr,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.create(vlmGrpData);
        ((ResourceGroupData) rscGrp).putVolumeGroup(accCtx, vlmGrpData);

        return vlmGrpData;
    }
}
