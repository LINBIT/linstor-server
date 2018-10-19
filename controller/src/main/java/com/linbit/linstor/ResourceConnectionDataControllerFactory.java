package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
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

import java.sql.SQLException;
import java.util.UUID;

@Singleton
public class ResourceConnectionDataControllerFactory
{
    private final ResourceConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionDataControllerFactory(
        ResourceConnectionDataDatabaseDriver dbDriverRef,
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

    public ResourceConnectionData create(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        ResourceConnection.RscConnFlags[] initFlags
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
    {
        sourceResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        ResourceConnectionData rscConData = ResourceConnectionData.get(accCtx, sourceResource, targetResource);

        if (rscConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceConnection already exists");
        }

        rscConData = new ResourceConnectionData(
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

        sourceResource.setResourceConnection(accCtx, rscConData);
        targetResource.setResourceConnection(accCtx, rscConData);

        return rscConData;
    }
}
