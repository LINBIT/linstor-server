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

public class StorPoolDataFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDataFactory(
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
        String storDriverSimpleClassNameRef
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
            false,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );
        driver.create(storPoolData);
        ((NodeData) node).addStorPool(accCtx, storPoolData);
        ((StorPoolDefinitionData) storPoolDef).addStorPool(accCtx, storPoolData);

        return storPoolData;
    }

    public StorPoolData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        StorPoolDefinition storPoolDef,
        String storDriverSimpleClassName
    )
        throws ImplementationError
    {
        StorPoolData storPoolData = null;

        try
        {
            storPoolData = (StorPoolData) node.getStorPool(accCtx, storPoolDef.getName());
            if (storPoolData == null)
            {
                storPoolData = new StorPoolData(
                    uuid,
                    node,
                    storPoolDef,
                    storDriverSimpleClassName,
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
