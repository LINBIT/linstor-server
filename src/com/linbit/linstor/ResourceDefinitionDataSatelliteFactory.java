package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class ResourceDefinitionDataSatelliteFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDefinitionDataSatelliteFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ResourceDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ResourceName rscName,
        TcpPortNumber portRef,
        ResourceDefinition.RscDfnFlags[] initFlags,
        String secret,
        ResourceDefinition.TransportType transType
    )
        throws ImplementationError
    {
        ResourceDefinitionData rscDfn = null;
        try
        {
            rscDfn = driver.load(rscName, false);
            if (rscDfn == null)
            {
                rscDfn = new ResourceDefinitionData(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false),
                    rscName,
                    portRef,
                    null,
                    StateFlagsBits.getMask(initFlags),
                    secret,
                    transType,
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>(),
                    new TreeMap<>()
                );
            }
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
