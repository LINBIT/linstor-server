package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class StorPoolControllerFactory
{
    private final StorPoolDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolControllerFactory(
        StorPoolDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public StorPool create(
        AccessContext accCtx,
        Node node,
        StorPoolDefinition storPoolDef,
        DeviceProviderKind deviceProviderKindRef,
        FreeSpaceTracker freeSpaceTrackerRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, IllegalStorageDriverException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPool storPool = null;

        storPool = node.getStorPool(accCtx, storPoolDef.getName());

        if (storPool != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        Node.Type nodeType = node.getNodeType(accCtx);
        if (!nodeType.isDeviceProviderKindAllowed(deviceProviderKindRef))
        {
            throw new IllegalStorageDriverException(
                "Illegal storage driver.",
                "The current node type '" + nodeType.name() + "' does not support the " +
                "storage driver '" + deviceProviderKindRef + "'.",
                null,
                "The allowed storage drivers for the current node type are:\n   " +
                nodeType.getAllowedKindClasses(),
                null
            );
        }
        storPool = new StorPool(
            UUID.randomUUID(),
            node,
            storPoolDef,
            deviceProviderKindRef,
            freeSpaceTrackerRef,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(storPool);
        freeSpaceTrackerRef.add(accCtx, storPool);
        node.addStorPool(accCtx, storPool);
        ((StorPoolDefinitionData) storPoolDef).addStorPool(accCtx, storPool);

        return storPool;
    }
}
