package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDataSatelliteFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolDataSatelliteFactory(
        StorPoolDataDatabaseDriver driverRef,
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

    public StorPoolData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        StorPoolDefinition storPoolDef,
        String storDriverSimpleClassName,
        FreeSpaceTracker freeSpaceTrackerRef
    )
        throws ImplementationError
    {
        StorPoolData storPoolData = null;

        try
        {
            storPoolData = (StorPoolData) node.getStorPool(accCtx, storPoolDef.getName());
            if (storPoolData == null)
            {
                FreeSpaceTracker fsm;
                if (freeSpaceTrackerRef == null)
                {
                    fsm = freeSpaceMgrFactory.getInstance(
                    );
                }
                else
                {
                    fsm = freeSpaceTrackerRef;
                }
                storPoolData = new StorPoolData(
                    uuid,
                    node,
                    storPoolDef,
                    storDriverSimpleClassName,
                    fsm,
                    true,
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>()
                );
                ((NodeData) node).addStorPool(accCtx, storPoolData);
                ((StorPoolDefinitionData) storPoolDef).addStorPool(accCtx, storPoolData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }


        return storPoolData;
    }
}
