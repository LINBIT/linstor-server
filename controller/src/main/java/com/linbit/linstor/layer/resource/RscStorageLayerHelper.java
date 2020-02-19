package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@Singleton
class RscStorageLayerHelper extends AbsRscLayerHelper<
    StorageRscData<Resource>, VlmProviderObject<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    RscStorageLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL)  DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            // StorageRscData.class cannot directly be casted to Class<StorageRscData<Resource>>. because java.
            // its type is Class<StorageRscData> (without nested types), but that is not enough as the super constructor
            // wants a Class<RSC_PO>, where RSC_PO is StorageRscData<Resource>.
            (Class<StorageRscData<Resource>>) ((Object) StorageRscData.class),
            DeviceLayerKind.STORAGE,
            rscLayerDataFactory
        );
    }

    @Override
    protected RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // StorageLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Storage specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // StorageLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Storage specific volume-definition, nothing to merge
    }

    @Override
    protected StorageRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            parentObjectRef,
            rscRef,
            rscNameSuffixRef
        );
    }

    @Override
    protected List<ChildResourceData> getChildRsc(
        StorageRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException
    {
        return Collections.emptyList(); // no children.
    }

    @Override
    protected void mergeRscData(StorageRscData<Resource> rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        throw new ImplementationError("Storage layer should not have child volumes to be asked for");
    }

    @Override
    protected VlmProviderObject<Resource> createVlmLayerData(
        StorageRscData<Resource> rscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException, InvalidKeyException, InvalidNameException
    {
        StorPool storPool = layerDataHelperProvider.get().getStorPool(vlm, rscData, payload);

        DeviceProviderKind kind = storPool.getDeviceProviderKind();
        VlmProviderObject<Resource> vlmData = rscData.getVlmProviderObject(
            vlm.getVolumeDefinition().getVolumeNumber()
        );
        if (vlmData == null)
        {
            switch (kind)
            {
                case DISKLESS:
                    vlmData = layerDataFactory.createDisklessData(
                        vlm,
                        vlm.getVolumeDefinition().getVolumeSize(apiCtx),
                        rscData,
                        storPool
                    );
                    break;
                case OPENFLEX_TARGET:
                    vlmData = layerDataFactory.createOpenflexTargetData(
                        vlm,
                        rscData,
                        storPool
                    );
                case LVM:
                    vlmData = layerDataFactory.createLvmData(vlm, rscData, storPool);
                    break;
                case LVM_THIN:
                    vlmData = layerDataFactory.createLvmThinData(vlm, rscData, storPool);
                    break;
                case ZFS: // fall-through
                case ZFS_THIN:
                    vlmData = layerDataFactory.createZfsData(vlm, rscData, kind, storPool);
                    break;
                case FILE: // fall-through
                case FILE_THIN:
                    vlmData = layerDataFactory.createFileData(vlm, rscData, kind, storPool);
                    break;
                case SPDK:
                    vlmData = layerDataFactory.createSpdkData(vlm, rscData, storPool);
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
                default:
                    throw new ImplementationError("Unexpected kind: " + kind);
            }
            storPool.putVolume(apiCtx, vlmData);
        }
        return vlmData;
    }

    @Override
    protected void mergeVlmData(
        VlmProviderObject<Resource> vlmDataRef,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws InvalidKeyException, InvalidNameException, DatabaseException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException, LinStorException
    {
        // if storage pool changed (i.e. because of a toggle disk) we need to update that

        StorPool currentStorPool = vlmDataRef.getStorPool();

        StorageRscData<Resource> storageRscData = (StorageRscData<Resource>) vlmDataRef
            .getRscLayerObject();
        StorPool newStorPool = layerDataHelperProvider.get().getStorPool(
            vlmRef,
            storageRscData,
            payloadRef
        );

        if (newStorPool != null && !newStorPool.equals(currentStorPool))
        {
            VlmProviderObject<Resource> vlmData = vlmDataRef;
            if (!currentStorPool.getDeviceProviderKind().equals(newStorPool.getDeviceProviderKind()))
            {
                // if the kind changes, we basically need a new vlmData
                vlmData = createVlmLayerData(
                    storageRscData,
                    vlmRef,
                    payloadRef,
                    layerListRef
                );
                VolumeNumber vlmNr = vlmData.getVlmNr();
                storageRscData.remove(apiCtx, vlmNr);
                storageRscData.getVlmLayerObjects().put(vlmNr, vlmData);
            }
            else
            {
                vlmDataRef.setStorPool(apiCtx, newStorPool);
            }
        }
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // changing storage pools allows other DeviceProviders than before. Therefore we simply delete
        // all storage volumes as they will be re-created soon

        HashSet<VolumeNumber> vlmNrs = new HashSet<>(rscDataRef.getVlmLayerObjects().keySet());
        for (VolumeNumber vlmNr : vlmNrs)
        {
            rscDataRef.remove(apiCtx, vlmNr);
        }
    }

    @Override
    protected boolean isExpectedToProvideDevice(StorageRscData<Resource> storageRscData) throws AccessDeniedException
    {
        return true;
    }

    @Override
    protected RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef
    )
    {
        // StorageLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<Snapshot> fromSnapVlmDataRef
    )
    {
        // StorageLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected StorageRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            rscParentRef,
            rscRef,
            fromSnapDataRef.getResourceNameSuffix()
        );
    }

    @Override
    protected VlmProviderObject<Resource> restoreVlmData(
        Volume vlmRef,
        StorageRscData<Resource> storRscData,
        VlmProviderObject<Snapshot> snapVlmData
    )
        throws DatabaseException, AccessDeniedException
    {
        VlmProviderObject<Resource> vlmData;

        DeviceProviderKind providerKind = snapVlmData.getProviderKind();
        StorPool storPool = snapVlmData.getStorPool();
        switch (providerKind)
        {
            case DISKLESS:
                vlmData = layerDataFactory.createDisklessData(
                    vlmRef,
                    snapVlmData.getUsableSize(),
                    storRscData,
                    storPool
                );
                break;
            case OPENFLEX_TARGET:
                throw new ImplementationError("Restoring from snapshots is not supported for openflex-setups");
            case FILE:
            case FILE_THIN:
                vlmData = layerDataFactory.createFileData(vlmRef, storRscData, providerKind, storPool);
                break;
            case LVM:
                vlmData = layerDataFactory.createLvmData(vlmRef, storRscData, storPool);
                break;
            case LVM_THIN:
                vlmData = layerDataFactory.createLvmThinData(vlmRef, storRscData, storPool);
                break;
            case ZFS:
            case ZFS_THIN:
                vlmData = layerDataFactory.createZfsData(vlmRef, storRscData, providerKind, storPool);
                break;
            case SPDK:
                vlmData = layerDataFactory.createSpdkData(vlmRef, storRscData, storPool);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected kind: " + kind);
        }
        storPool.putVolume(apiCtx, vlmData);
        return vlmData;
    }
}
