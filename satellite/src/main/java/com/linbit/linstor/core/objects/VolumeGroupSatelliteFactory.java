package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class VolumeGroupSatelliteFactory
{
    private final VolumeGroupDatabaseDriver vlmGrpDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeGroupSatelliteFactory(
        VolumeGroupDatabaseDriver vlmGrpDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        vlmGrpDriver = vlmGrpDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public VolumeGroup getInstanceSatellite(
        UUID uuid,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr,
        long initFlags
    )
        throws DatabaseException
    {
        VolumeGroup vlmGrp = rscGrp.getVolumeGroup(vlmNr);

        if (vlmGrp == null)
        {
            vlmGrp = new VolumeGroup(
                uuid,
                rscGrp,
                vlmNr,
                initFlags,
                vlmGrpDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            rscGrp.putVolumeGroup(vlmGrp);
        }
        return vlmGrp;
    }
}
