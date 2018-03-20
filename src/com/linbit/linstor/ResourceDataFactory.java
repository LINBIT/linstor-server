package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDataFactory
{
    private final ResourceDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final VolumeDataFactory volumeDataFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDataFactory(
        ResourceDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ResourceData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node,
        NodeId nodeId,
        Resource.RscFlags[] initFlags,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        ResourceData resData = null;


        resData = dbDriver.load(node, resDfn, false);

        if (failIfExists && resData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Resource already exists");
        }

        if (resData == null && createIfNotExists)
        {
            resData = new ResourceData(
                UUID.randomUUID(),
                objectProtectionFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(
                        node.getName(),
                        resDfn.getName()
                    ),
                    true
                ),
                resDfn,
                node,
                nodeId,
                StateFlagsBits.getMask(initFlags),
                dbDriver,
                propsContainerFactory,
                volumeDataFactory,
                transObjFactory,
                transMgrProvider,
                new TreeMap<>(),
                new TreeMap<>()
            );
            dbDriver.create(resData);
            ((NodeData) node).addResource(accCtx, resData);
            ((ResourceDefinitionData) resDfn).addResource(accCtx, resData);
        }
        return resData;
    }

    public ResourceData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        ResourceDefinition rscDfn,
        NodeId nodeId,
        Resource.RscFlags[] initFlags
    )
        throws ImplementationError
    {
        ResourceData rscData;
        try
        {
            rscData = dbDriver.load(node, rscDfn, false);
            if (rscData == null)
            {
                rscData = new ResourceData(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false),
                    rscDfn,
                    node,
                    nodeId,
                    StateFlagsBits.getMask(initFlags),
                    dbDriver,
                    propsContainerFactory,
                    volumeDataFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>()
                );
                ((NodeData) node).addResource(accCtx, rscData);
                ((ResourceDefinitionData) rscDfn).addResource(accCtx, rscData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscData;
    }
}
