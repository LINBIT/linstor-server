package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class ResourceSatelliteFactory
{
    private final ResourceDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceSatelliteFactory(
        ResourceDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public Resource getInstanceSatellite(
        UUID uuid,
        Node node,
        ResourceDefinition rscDfn,
        Resource.Flags[] initFlags
    )
        throws ImplementationError
    {
        Resource rscData;
        try
        {
            rscData = node.getResource(rscDfn.getName());
            if (rscData == null)
            {
                rscData = new Resource(
                    uuid,
                    rscDfn,
                    node,
                    StateFlagsBits.getMask(initFlags),
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>(),
                    null
                );
                node.addResource(rscData);
                rscDfn.addResource(rscData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscData;
    }
}
