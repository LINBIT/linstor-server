package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

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
        ResourceConnection.Flags[] initFlags
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        sourceResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        ResourceConnection rscConData = ResourceConnection.get(accCtx, sourceResource, targetResource);

        if (rscConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceConnection already exists");
        }

        rscConData = new ResourceConnection(
            UUID.randomUUID(),
            sourceResource,
            targetResource,
            null,
            tcpPortPool,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            StateFlagsBits.getMask(initFlags)
        );
        dbDriver.create(rscConData);

        sourceResource.setAbsResourceConnection(accCtx, rscConData);
        targetResource.setAbsResourceConnection(accCtx, rscConData);

        return rscConData;
    }
}
