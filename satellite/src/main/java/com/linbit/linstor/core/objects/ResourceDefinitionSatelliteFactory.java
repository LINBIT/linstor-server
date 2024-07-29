package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDefinitionSatelliteFactory
{
    private final ResourceDefinitionDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ResourceDefinitionMap rscDfnMap;

    @Inject
    public ResourceDefinitionSatelliteFactory(
        ResourceDefinitionDatabaseDriver driverRef,
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

    public ResourceDefinition getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ResourceGroup rscGrpRef,
        ResourceName rscName,
        @Nullable ResourceDefinition.Flags[] initFlags
    )
        throws ImplementationError
    {
        ResourceDefinition rscDfn;
        try
        {
            // we should have system context anyways, so we skip the objProt-check
            rscDfn = rscDfnMap.get(rscName);
            if (rscDfn == null)
            {
                rscDfn = new ResourceDefinition(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false),
                    rscName,
                    null,
                    StateFlagsBits.getMask(initFlags),
                    new ArrayList<>(), // satellite does not care about the layer stack for new resources
                    // as every resource has already a complete tree of layerdata
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>(),
                    new TreeMap<>(),
                    new TreeMap<>(),
                    rscGrpRef
                );
                rscGrpRef.addResourceDefinition(accCtx, rscDfn);
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
