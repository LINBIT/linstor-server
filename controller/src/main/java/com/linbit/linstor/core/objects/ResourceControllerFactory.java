package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceControllerFactory
{
    private final ResourceDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlRscLayerDataFactory layerStackHelper;

    @Inject
    public ResourceControllerFactory(
        ResourceDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlRscLayerDataFactory layerStackHelperRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
    }

    public Resource create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        Integer nodeIdIntRef,
        Resource.Flags[] initFlags,
        List<DeviceLayerKind> layerStackRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Resource rscData = createEmptyResource(accCtx, rscDfn, node, initFlags);

        List<DeviceLayerKind> layerStack = layerStackRef;
        if (layerStack.isEmpty())
        {
            layerStack = rscDfn.getLayerStack(accCtx);
            if (layerStack.isEmpty())
            {
                layerStack = layerStackHelper.createDefaultStack(accCtx, rscData);
                rscDfn.setLayerStack(accCtx, layerStack);
            }
        }
        if (!layerStack.get(layerStack.size() - 1).equals(DeviceLayerKind.STORAGE))
        {
            throw new ImplementationError("Lowest layer has to be a STORAGE layer. " + new ArrayList<>(layerStack));
        }

        // TODO: might be a good idea to create this object earlier
        LayerPayload payload = new LayerPayload();
        payload.getDrbdRsc().nodeId = nodeIdIntRef;
        layerStackHelper.ensureStackDataExists(rscData, layerStack, payload);

        return rscData;
    }

    public Resource create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        AbsRscLayerObject<Snapshot> snapLayerData
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        Resource rscData = createEmptyResource(accCtx, rscDfn, node, new Resource.Flags[0]);
        layerStackHelper.copyLayerData(snapLayerData, rscData);

        return rscData;
    }

    private Resource createEmptyResource(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        Resource.Flags[] initFlags
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        Resource rscData = node.getResource(accCtx, rscDfn.getName());

        if (rscData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Resource already exists");
        }

        rscData = new Resource(
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
            StateFlagsBits.getMask(initFlags),
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );

        dbDriver.create(rscData);
        node.addResource(accCtx, rscData);
        rscDfn.addResource(accCtx, rscData);
        return rscData;
    }
}
