package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class VolumeDefinitionSatelliteFactory
{
    private final VolumeDefinitionDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects stltSecObjs;

    @Inject
    public VolumeDefinitionSatelliteFactory(
        VolumeDefinitionDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjsRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObjs = stltSecObjsRef;
    }

    public VolumeDefinition getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmDfnUuid,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        long vlmSize,
        VolumeDefinition.Flags[] flags
    )
        throws ImplementationError
    {
        VolumeDefinition vlmDfnData;
        try
        {
            vlmDfnData = (VolumeDefinition) rscDfn.getVolumeDfn(accCtx, vlmNr);
            if (vlmDfnData == null)
            {
                vlmDfnData = new VolumeDefinition(
                    vlmDfnUuid,
                    rscDfn,
                    vlmNr,
                    vlmSize,
                    StateFlagsBits.getMask(flags),
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>()
                );
                ((ResourceDefinition) rscDfn).putVolumeDefinition(accCtx, vlmDfnData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return vlmDfnData;
    }
}
