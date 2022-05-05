package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class VolumeGroupSatelliteFactory
{
    private final AccessContext sysCtx;
    private final ResourceGroupDatabaseDriver rscGrpDriver;
    private final VolumeGroupDatabaseDriver vlmGrpDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.ResourceGroupMap rscGrpMap;

    @Inject
    public VolumeGroupSatelliteFactory(
        @SystemContext AccessContext sysCtxRef,
        ResourceGroupDatabaseDriver rscGrpDriverRef,
        VolumeGroupDatabaseDriver vlmGrpDriverRef,
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

    public VolumeGroup getInstanceSatellite(
        UUID uuid,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr,
        long initFlags
    )
        throws DatabaseException, AccessDeniedException
    {
        VolumeGroup vlmGrp = rscGrp.getVolumeGroup(sysCtx, vlmNr);

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
            rscGrp.putVolumeGroup(sysCtx, vlmGrp);
        }
        return vlmGrp;
    }
}
