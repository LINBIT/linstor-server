package com.linbit.linstor;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class VolumeGroupDataSatelliteFactory
{
    private final AccessContext sysCtx;
    private final ResourceGroupDataDatabaseDriver rscGrpDriver;
    private final VolumeGroupDataDatabaseDriver vlmGrpDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.ResourceGroupMap rscGrpMap;

    @Inject
    public VolumeGroupDataSatelliteFactory(
        @SystemContext AccessContext sysCtxRef,
        ResourceGroupDataDatabaseDriver rscGrpDriverRef,
        VolumeGroupDataDatabaseDriver vlmGrpDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.ResourceGroupMap rscGrpMapRef
    )
    {
        sysCtx = sysCtxRef;
        rscGrpDriver = rscGrpDriverRef;
        vlmGrpDriver = vlmGrpDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        rscGrpMap = rscGrpMapRef;
    }

    public VolumeGroupData getInstanceSatellite(
        UUID uuid,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr
    )
        throws DatabaseException, AccessDeniedException
    {
        VolumeGroupData vlmGrp = (VolumeGroupData) rscGrp.getVolumeGroup(sysCtx, vlmNr);

        if (vlmGrp == null)
        {
            vlmGrp = new VolumeGroupData(
                uuid,
                rscGrp,
                vlmNr,
                vlmGrpDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
        }
        return vlmGrp;
    }
}
