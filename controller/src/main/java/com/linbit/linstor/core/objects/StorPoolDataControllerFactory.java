package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.Node.NodeType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
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

public class StorPoolDataControllerFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDataControllerFactory(
        StorPoolDataDatabaseDriver driverRef,
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

    public StorPoolData create(
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
        StorPoolData storPoolData = null;

        storPoolData = (StorPoolData) node.getStorPool(accCtx, storPoolDef.getName());

        if (storPoolData != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        NodeType nodeType = node.getNodeType(accCtx);
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
        storPoolData = new StorPoolData(
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
        driver.create(storPoolData);
        freeSpaceTrackerRef.add(accCtx, storPoolData);
        ((NodeData) node).addStorPool(accCtx, storPoolData);
        ((StorPoolDefinitionData) storPoolDef).addStorPool(accCtx, storPoolData);

        return storPoolData;
    }
}
