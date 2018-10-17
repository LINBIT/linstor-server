package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.ComparatorUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Comparator;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDefinitionDataSatelliteFactory
{
    private final ResourceDefinitionDataDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionMap rscDfnMap;

    @Inject
    public ResourceDefinitionDataSatelliteFactory(
        ResourceDefinitionDataDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        rscDfnMap = rscDfnMapRef;
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
        ResourceDefinitionData rscDfn;
        try
        {
            // we should have system context anyways, so we skip the objProt-check
            rscDfn = (ResourceDefinitionData) rscDfnMap.get(rscName);
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
                    new TreeMap<>(),
                    new TreeMap<>(ComparatorUtils::compareClassesByFQN)
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
        new Comparator<Class<?>>()
        {

            @Override
            public int compare(Class<?> o1, Class<?> o2)
            {
                // TODO Auto-generated method stub
                throw new ImplementationError("Not implemented yet");
            }
        };
        return rscDfn;
    }
}
