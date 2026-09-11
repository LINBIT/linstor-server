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
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public class ResourceControllerFactory
{
    private final ResourceDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlRscLayerDataFactory layerStackHelper;

    @Inject
    public ResourceControllerFactory(
        ResourceDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlRscLayerDataFactory layerStackHelperRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        layerStackHelper = layerStackHelperRef;
    }

    public Resource create(
        ResourceDefinition rscDfn,
        Node node,
        @Nullable LayerPayload payload,
        @Nullable Resource.Flags[] initFlags,
        List<DeviceLayerKind> layerStackRef
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        Resource rscData = createEmptyResource(rscDfn, node, initFlags, layerStackRef, false);

        List<DeviceLayerKind> layerStack = layerStackRef;
        List<DeviceLayerKind> rscDfnLayerStack = rscDfn.getLayerStack();
        if (layerStack.isEmpty())
        {
            if (rscDfnLayerStack.isEmpty())
            {
                rscDfnLayerStack = layerStackHelper.createDefaultStack(rscData);
                rscDfn.setLayerStack(rscDfnLayerStack);
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
            rscDfn.setLayerStack(rscDfnLayerStack);
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
        ResourceDefinition rscDfn,
        Node node,
        AbsRscLayerObject<RSC> absLayerData,
        Resource.Flags[] flags,
        boolean fromBackup,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws LinStorDataAlreadyExistsException, DatabaseException
    {
        Resource rscData = createEmptyResource(
            rscDfn,
            node,
            flags,
            LayerRscUtils.getLayerStack(absLayerData),
            fromBackup
        );
        layerStackHelper.copyLayerData(absLayerData, rscData, storpoolRenameMap, apiCallRc);

        return rscData;
    }

    private Resource createEmptyResource(
        ResourceDefinition rscDfn,
        Node node,
        @Nullable Resource.Flags[] initFlags,
        List<DeviceLayerKind> expectedLayerStack,
        boolean fromBackup
    )
        throws LinStorDataAlreadyExistsException, DatabaseException
    {
        Resource rsc = node.getResource(rscDfn.getName());
        if (!fromBackup)
        {
            ensureResourceNotRestoring(rscDfn);
        }

        if (rsc == null)
        {
            rsc = new Resource(
                UUID.randomUUID(),
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
                Instant.ofEpochMilli(AbsResource.CREATE_DATE_INIT_VALUE)
            );

            dbDriver.create(rsc);
            node.addResource(rsc);
            rscDfn.addResource(rsc);
        }
        else
        {
            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            if (rscFlags.isSomeSet(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rsc);
                if (!layerStack.equals(expectedLayerStack))
                {
                    throw new LinStorDataAlreadyExistsException(
                        "Resource already exists with different layerstack. Expected layerstack: " +
                            expectedLayerStack +
                        ", existing layerstack: " + layerStack
                    );
                }
                rscFlags.disableFlags(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    vlm.getFlags().disableFlags(Volume.Flags.DELETE, Volume.Flags.DRBD_DELETE);
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

    private void ensureResourceNotRestoring(ResourceDefinition rscDfn)
    {
        if (rscDfn.getFlags().isSet(ResourceDefinition.Flags.RESTORE_TARGET))
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
