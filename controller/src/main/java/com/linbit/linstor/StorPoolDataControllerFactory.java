package com.linbit.linstor;

import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.IllegalStorageDriverException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageDriverLoader;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

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
        String storDriverSimpleClassNameRef,
        FreeSpaceTracker freeSpaceTrackerRef
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException, IllegalStorageDriverException
    {
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;

        storPoolData = (StorPoolData) node.getStorPool(accCtx, storPoolDef.getName());

        if (storPoolData != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        StorageDriverKind storageDriverKind = StorageDriverLoader.getKind(storDriverSimpleClassNameRef);
        NodeType nodeType = node.getNodeType(accCtx);
        if (!nodeType.isStorageKindAllowed(storageDriverKind))
        {
            throw new IllegalStorageDriverException(
                "Illegal storage driver.",
                "The current node type '" + nodeType.name() + "' does not support the " +
                "storage driver '" + storDriverSimpleClassNameRef + "'.",
                null,
                "The allowed storage drivers for the current node type are:\n   " +
                nodeType.getAllowedKindClasses().stream()
                    .map(
                        clazz ->
                        {
                            String simpleName = clazz.getSimpleName();
                            int kindIdx = simpleName.lastIndexOf("Kind");
                            if (kindIdx > 0)
                            {
                                simpleName = simpleName.substring(0, kindIdx);
                            }
                            return simpleName;
                        }
                    )
                    .collect(Collectors.joining("\n   ")),
                null
            );
        }
        storPoolData = new StorPoolData(
            UUID.randomUUID(),
            node,
            storPoolDef,
            storageDriverKind,
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
