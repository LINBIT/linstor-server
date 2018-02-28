package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;

import javax.inject.Inject;
import java.util.UUID;

public class ResourceDefinitionDataSatelliteFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public ResourceDefinitionDataSatelliteFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public ResourceDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ResourceName rscName,
        TcpPortNumber portRef,
        ResourceDefinition.RscDfnFlags[] initFlags,
        String secret,
        ResourceDefinition.TransportType transType,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceDefinitionData rscDfn = null;
        try
        {
            rscDfn = driver.load(rscName, false, transMgr);
            if (rscDfn == null)
            {
                rscDfn = new ResourceDefinitionData(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false, transMgr),
                    rscName,
                    portRef,
                    null,
                    StateFlagsBits.getMask(initFlags),
                    secret,
                    transType,
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            rscDfn.initialized();
            rscDfn.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscDfn;
    }
}
