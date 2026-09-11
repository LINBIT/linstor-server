package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class ResourceConnectionControllerFactory
{
    private final ResourceConnectionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionControllerFactory(
        ResourceConnectionDatabaseDriver dbDriverRef,
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

    public ResourceConnection create(
        Resource sourceResource,
        Resource targetResource,
        @Nullable ResourceConnection.Flags[] initFlags
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        ResourceConnection rscConData = ResourceConnection.createWithSorting(
            UUID.randomUUID(),
            sourceResource,
            targetResource,
            null,
            null,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            StateFlagsBits.getMask(initFlags)
        );
        dbDriver.create(rscConData);

        sourceResource.setAbsResourceConnection(rscConData);
        targetResource.setAbsResourceConnection(rscConData);

        return rscConData;
    }
}
