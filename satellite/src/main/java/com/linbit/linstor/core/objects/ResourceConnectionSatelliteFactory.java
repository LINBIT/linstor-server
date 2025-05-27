package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class ResourceConnectionSatelliteFactory
{
    private final ResourceConnectionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionSatelliteFactory(
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

    public ResourceConnection getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Resource sourceResource,
        Resource targetResource,
        ResourceConnection.Flags[] initFlags,
        @Nullable TcpPortNumber drbdProxyPortSrcRef,
        @Nullable TcpPortNumber drbdProxyPortTargetRef
    )
        throws ImplementationError
    {
        ResourceConnection rscConData;

        try
        {
            rscConData = sourceResource.getAbsResourceConnection(accCtx, targetResource);

            if (rscConData == null)
            {
                rscConData = ResourceConnection.createWithSorting(
                    uuid,
                    sourceResource,
                    targetResource,
                    drbdProxyPortSrcRef,
                    drbdProxyPortTargetRef,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    StateFlagsBits.getMask(initFlags),
                    accCtx
                );
                sourceResource.setAbsResourceConnection(accCtx, rscConData);
                targetResource.setAbsResourceConnection(accCtx, rscConData);
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
