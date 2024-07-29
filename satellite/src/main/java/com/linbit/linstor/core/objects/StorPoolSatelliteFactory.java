package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class StorPoolSatelliteFactory
{
    private final StorPoolDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolSatelliteFactory(
        StorPoolDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
    }

    public StorPool getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        StorPoolDefinition storPoolDef,
        @Nonnull DeviceProviderKind providerKind,
        FreeSpaceTracker freeSpaceTrackerRef,
        boolean externalLocking
    )
        throws ImplementationError
    {
        StorPool storPool = null;

        try
        {
            storPool = node.getStorPool(accCtx, storPoolDef.getName());
            if (storPool == null)
            {
                FreeSpaceTracker fsm;
                if (freeSpaceTrackerRef == null)
                {
                    fsm = freeSpaceMgrFactory.getInstance(
                        new SharedStorPoolName(node.getName(), storPoolDef.getName()));
                }
                else
                {
                    fsm = freeSpaceTrackerRef;
                }
                storPool = new StorPool(
                    uuid,
                    node,
                    storPoolDef,
                    providerKind,
                    fsm,
                    externalLocking,
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>()
                );
                node.addStorPool(accCtx, storPool);
                storPoolDef.addStorPool(accCtx, storPool);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }


        return storPool;
    }
}
