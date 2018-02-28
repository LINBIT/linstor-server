package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.util.UUID;

public class VolumeDefinitionDataSatelliteFactory
{
    private final VolumeDefinitionDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public VolumeDefinitionDataSatelliteFactory(
        VolumeDefinitionDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public VolumeDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID vlmDfnUuid,
        ResourceDefinition rscDfn,
        VolumeNumber vlmNr,
        long vlmSize,
        MinorNumber minorNumber,
        VolumeDefinition.VlmDfnFlags[] flags,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        VolumeDefinitionData vlmDfnData;
        try
        {
            vlmDfnData = driver.load(rscDfn, vlmNr, false, transMgr);
            if (vlmDfnData == null)
            {
                vlmDfnData = new VolumeDefinitionData(
                    vlmDfnUuid,
                    accCtx,
                    rscDfn,
                    vlmNr,
                    minorNumber,
                    null,
                    vlmSize,
                    StateFlagsBits.getMask(flags),
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            vlmDfnData.initialized();
            vlmDfnData.setConnection(transMgr);
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
