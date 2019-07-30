package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.objects.ResourceConnectionKey;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
public class ResourceConnectionDataSatelliteFactory
{
    private final ResourceConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionDataSatelliteFactory(
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

    public ResourceConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Resource sourceResource,
        Resource targetResource,
        ResourceConnection.RscConnFlags[] initFlags,
        TcpPortNumber portRef
    )
        throws ImplementationError
    {
        ResourceConnectionData rscConData;
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
                    portRef,
                    null,
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
