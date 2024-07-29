package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class ResourceConnectionControllerFactory
{
    private final ResourceConnectionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionControllerFactory(
        ResourceConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ResourceConnection create(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        @Nullable ResourceConnection.Flags[] initFlags
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        ResourceConnection rscConData = ResourceConnection.createWithSorting(
            UUID.randomUUID(),
            sourceResource,
            targetResource,
            null,
            tcpPortPool,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            StateFlagsBits.getMask(initFlags),
            accCtx
        );
        dbDriver.create(rscConData);

        sourceResource.setAbsResourceConnection(accCtx, rscConData);
        targetResource.setAbsResourceConnection(accCtx, rscConData);

        return rscConData;
    }
}
