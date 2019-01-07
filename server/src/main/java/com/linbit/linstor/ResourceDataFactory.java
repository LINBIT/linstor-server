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
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDataFactory
{
    private final ResourceDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDataFactory(
        ResourceDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @RemoveAfterDevMgrRework
    public ResourceData create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        NodeId nodeId,
        Resource.RscFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        return createTyped(
            accCtx,
            rscDfn,
            node,
            nodeId,
            initFlags,
            ResourceType.DEFAULT // this state will be remove when rework is finished
        );
    }

    public ResourceData createTyped(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        NodeId nodeId,
        Resource.RscFlags[] initFlags,
        ResourceType type
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        ResourceData rscData = (ResourceData) node.getResource(accCtx, rscDfn.getName(), type);

        if (rscData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Resource already exists");
        }

        rscData = new ResourceData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(
                    node.getName(),
                    rscDfn.getName()
                ),
                true
            ),
            rscDfn,
            node,
            nodeId,
            StateFlagsBits.getMask(initFlags),
            type,
            new ArrayList<>(),
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new HashMap<>() // LayerDataStorage uses Class as key, which is not comparable -> no TreeMap
        );
        dbDriver.create(rscData);
        ((NodeData) node).addResource(accCtx, rscData);
        ((ResourceDefinitionData) rscDfn).addResource(accCtx, rscData);

        return rscData;
    }

    @RemoveAfterDevMgrRework
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
        return getTypedInstanceSatellite(
            accCtx,
            uuid,
            node,
            rscDfn,
            nodeId,
            initFlags,
            ResourceType.DEFAULT,
            new ArrayList<>()
        );
    }

    public ResourceData getTypedInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node,
        ResourceDefinition rscDfn,
        NodeId nodeId,
        Resource.RscFlags[] initFlags,
        ResourceType type,
        List<Resource> children
    )
        throws ImplementationError
    {
        ResourceData rscData;
        try
        {
            rscData = (ResourceData) node.getResource(accCtx, rscDfn.getName(), type);
            if (rscData == null)
            {
                rscData = new ResourceData(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", false),
                    rscDfn,
                    node,
                    nodeId,
                    StateFlagsBits.getMask(initFlags),
                    type,
                    children,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>(),
                    new HashMap<>() // LayerDataStorage uses Class as key, which is not comparable -> no TreeMap
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
