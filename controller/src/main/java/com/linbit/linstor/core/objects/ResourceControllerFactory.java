package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource.Flags;
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
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

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
        @Nullable LayerPayload payload,
        @Nullable Resource.Flags[] initFlags,
        List<DeviceLayerKind> layerStackRef
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Resource rscData = createEmptyResource(accCtx, rscDfn, node, initFlags, layerStackRef, false);

        List<DeviceLayerKind> layerStack = layerStackRef;
        List<DeviceLayerKind> rscDfnLayerStack = rscDfn.getLayerStack(accCtx);
        if (layerStack.isEmpty())
        {
            if (rscDfnLayerStack.isEmpty())
            {
                rscDfnLayerStack = layerStackHelper.createDefaultStack(accCtx, rscData);
                rscDfn.setLayerStack(accCtx, rscDfnLayerStack);
                layerStack = rscDfnLayerStack;
            }
            else
            {
                layerStack = rscDfnLayerStack;
            }
        }
        else
        if (rscDfnLayerStack.isEmpty())
        {
            rscDfnLayerStack = layerStack;
            rscDfn.setLayerStack(accCtx, rscDfnLayerStack);
        }
        DeviceLayerKind lowestLayer = layerStack.get(layerStack.size() - 1);
        if (!lowestLayer.equals(DeviceLayerKind.STORAGE))
        {
            throw new ImplementationError(
                "Lowest layer has to be a STORAGE layer. " + new ArrayList<>(layerStack)
            );
        }

        layerStackHelper.ensureStackDataExists(rscData, layerStack, payload == null ? new LayerPayload() : payload);

        return rscData;
    }

    public <RSC extends AbsResource<RSC>> Resource create(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        AbsRscLayerObject<RSC> absLayerData,
        Resource.Flags[] flags,
        boolean fromBackup,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        Resource rscData = createEmptyResource(
            accCtx,
            rscDfn,
            node,
            flags,
            LayerRscUtils.getLayerStack(absLayerData, accCtx),
            fromBackup
        );
        layerStackHelper.copyLayerData(absLayerData, rscData, storpoolRenameMap, apiCallRc);

        return rscData;
    }

    private Resource createEmptyResource(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        Node node,
        @Nullable Resource.Flags[] initFlags,
        List<DeviceLayerKind> expectedLayerStack,
        boolean fromBackup
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        Resource rsc = node.getResource(accCtx, rscDfn.getName());
        if (!fromBackup)
        {
            ensureResourceNotRestoring(rscDfn, accCtx);
        }

        if (rsc == null)
        {
            rsc = new Resource(
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
                new TreeMap<>(),
                // use special epoch time to mark this as a new resource which will get set on resource apply
                new Date(AbsResource.CREATE_DATE_INIT_VALUE)
            );

            dbDriver.create(rsc);
            node.addResource(accCtx, rsc);
            rscDfn.addResource(accCtx, rsc);
        }
        else
        {
            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            if (rscFlags.isSomeSet(accCtx, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rsc, accCtx);
                if (!layerStack.equals(expectedLayerStack))
                {
                    throw new LinStorDataAlreadyExistsException(
                        "Resource already exists with different layerstack. Expected layerstack: " +
                            expectedLayerStack +
                        ", existing layerstack: " + layerStack
                    );
                }
                rscFlags.disableFlags(accCtx, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    vlm.getFlags().disableFlags(accCtx, Volume.Flags.DELETE, Volume.Flags.DRBD_DELETE);
                }

                ResourceDataUtils.recalculateVolatileRscData(layerStackHelper, rsc);
            }
            else
            {
                throw new LinStorDataAlreadyExistsException("Resource already exists");
            }
        }

        return rsc;
    }

    private void ensureResourceNotRestoring(ResourceDefinition rscDfn, AccessContext accCtx)
        throws AccessDeniedException
    {
        if (rscDfn.getFlags().isSet(accCtx, ResourceDefinition.Flags.RESTORE_TARGET))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    "Cannot create a resource in a resource definition that is currently " +
                        "restoring a snapshot or backup."
                )
            );
        }
    }
}
