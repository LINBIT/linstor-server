package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class ResourceConnectionDataFactory
{
    private final ResourceConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionDataFactory(
        ResourceConnectionDataDatabaseDriver dbDriverRef,
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

    public ResourceConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Resource sourceResource,
        Resource targetResource,
        ResourceConnection.RscConnFlags[] initFlags
    )
        throws ImplementationError
    {
        ResourceConnectionData rscConData = null;
        ResourceConnectionKey connectionKey = new ResourceConnectionKey(sourceResource, targetResource);

        try
        {
            rscConData = (ResourceConnectionData) sourceResource.getResourceConnection(accCtx, targetResource);

            if (rscConData == null)
            {
                rscConData = new ResourceConnectionData(
                    uuid,
                    connectionKey.getSource(),
                    connectionKey.getTarget(),
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    StateFlagsBits.getMask(initFlags)
                );
                sourceResource.setResourceConnection(accCtx, rscConData);
                targetResource.setResourceConnection(accCtx, rscConData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscConData;
    }
}
