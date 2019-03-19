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
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

public class ResourceDataControllerFactory
{
    private final ResourceDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlLayerStackHelper layerStackHelper;

    @Inject
    public ResourceDataControllerFactory(
        ResourceDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlLayerStackHelper layerStackHelperRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
    }

    public ResourceData create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        Integer nodeIdIntRef,
        Resource.RscFlags[] initFlags,
        List<DeviceLayerKind> layerStackRef
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        ResourceData rscData = (ResourceData) node.getResource(accCtx, rscDfn.getName());

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
            StateFlagsBits.getMask(initFlags),
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );


        dbDriver.create(rscData);
        ((NodeData) node).addResource(accCtx, rscData);
        ((ResourceDefinitionData) rscDfn).addResource(accCtx, rscData);

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
        layerStackHelper.ensureStackDataExists(rscData, layerStack, nodeIdIntRef);

        return rscData;
    }
}
