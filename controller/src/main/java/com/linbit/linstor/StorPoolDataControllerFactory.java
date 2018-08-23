package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDataControllerFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final FreeSpaceMgrControllerFactory freeSpaceMgrFactory;

    @Inject
    public StorPoolDataControllerFactory(
        StorPoolDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        FreeSpaceMgrControllerFactory freeSpaceMgrFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
    }

    public StorPoolData create(
        AccessContext accCtx,
        Node node,
        StorPoolDefinition storPoolDef,
        String storDriverSimpleClassNameRef,
        FreeSpaceTracker freeSpaceTrackerRef
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;

        storPoolData = (StorPoolData) node.getStorPool(accCtx, storPoolDef.getName());

        if (storPoolData != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        storPoolData = new StorPoolData(
            UUID.randomUUID(),
            node,
            storPoolDef,
            storDriverSimpleClassNameRef,
            freeSpaceTrackerRef,
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(storPoolData);
        freeSpaceTrackerRef.add(accCtx, storPoolData);
        ((NodeData) node).addStorPool(accCtx, storPoolData);
        ((StorPoolDefinitionData) storPoolDef).addStorPool(accCtx, storPoolData);

        return storPoolData;
    }
}
