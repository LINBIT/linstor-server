package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
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
import javax.inject.Singleton;

import java.util.Map;

@Singleton
class SnapStorageLayerHelper extends AbsSnapLayerHelper<
    StorageRscData<Snapshot>, VlmProviderObject<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    SnapStorageLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            DeviceLayerKind.STORAGE
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject createSnapDfnData(SnapshotDefinition rscDfnRef, String rscNameSuffixRef)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // StorageLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // StorageLayer does not have volume-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected StorageRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            parentRef,
            snapRef,
            rscDataRef.getResourceNameSuffix()
        );
    }

    @Override
    protected VlmProviderObject<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        StorageRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmDataRef
    )
        throws DatabaseException, AccessDeniedException
    {
        VlmProviderObject<Snapshot> snapVlmData;

        // we have to create layerdata even for providers that does not support snapshots
        // otherwise we are not persisting the provider kind and other meta-information which could
        // be required when restoring from a snapshot
        DeviceProviderKind providerKind = vlmDataRef.getProviderKind();
        StorPool storPool = vlmDataRef.getStorPool();
        switch (providerKind)
        {
            case DISKLESS:
                snapVlmData = layerDataFactory.createDisklessData(
                    snapVlmRef,
                    vlmDataRef.getUsableSize(),
                    snapDataRef,
                    storPool
                );
                break;
            case FILE: // fall-through
            case FILE_THIN:
                snapVlmData = layerDataFactory.createFileData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case LVM:
                snapVlmData = layerDataFactory.createLvmData(snapVlmRef, snapDataRef, storPool);
                break;
            case LVM_THIN:
                snapVlmData = layerDataFactory.createLvmThinData(snapVlmRef, snapDataRef, storPool);
                break;
            case STORAGE_SPACES: // fall-through
            case STORAGE_SPACES_THIN:
                snapVlmData = layerDataFactory.createStorageSpacesData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case ZFS: // fall-through
            case ZFS_THIN:
                snapVlmData = layerDataFactory.createZfsData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case SPDK:
            case REMOTE_SPDK:
                snapVlmData = layerDataFactory.createSpdkData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case EBS_INIT:
            case EBS_TARGET:
                snapVlmData = layerDataFactory.createEbsData(snapVlmRef, snapDataRef, storPool);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            default:
                throw new ImplementationError("Unexpected kind: " + kind);
        }
        // To restore backups, the usable size is needed in the snapshot
        snapVlmData.setUsableSize(vlmDataRef.getUsableSize());
        storPool.putSnapshotVolume(apiCtx, snapVlmData);
        return snapVlmData;
    }

    @Override
    protected @Nullable RscDfnLayerObject restoreSnapDfnData(
        SnapshotDefinition snapshotDefinitionRef,
        RscLayerDataApi rscLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, IllegalArgumentException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // StorageLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // StorageLayer does not have volume-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected StorageRscData<Snapshot> restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, AccessDeniedException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            parentRef,
            snapRef,
            rscLayerDataApiRef.getRscNameSuffix()
        );
    }

    @Override
    protected VlmProviderObject<Snapshot> restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        StorageRscData<Snapshot> snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        VlmProviderObject<Snapshot> snapVlmData;

        // we have to create layerdata even for providers that does not support snapshots
        // otherwise we are not persisting the provider kind and other meta-information which could
        // be required when restoring from a snapshot
        DeviceProviderKind providerKind = vlmLayerDataApiRef.getProviderKind();
        StorPool storPool = AbsLayerHelperUtils.getStorPool(
            apiCtx,
            snapVlmRef,
            snapDataRef,
            vlmLayerDataApiRef.getStorPoolApi().getStorPoolName(),
            renameStorPoolMapRef,
            apiCallRc
        );
        if (!storPool.getDeviceProviderKind().equals(providerKind))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_PROVIDER | ApiConsts.MASK_BACKUP,
                    "The storage pool " + storPool.getName().displayValue +
                    " does not have the expected provider: " + providerKind
                )
            );
        }
        switch (providerKind)
        {
            case DISKLESS:
                snapVlmData = layerDataFactory.createDisklessData(
                    snapVlmRef,
                    snapVlmRef.getVolumeSize(apiCtx),
                    snapDataRef,
                    storPool
                );
                break;
            case FILE: // fall-through
            case FILE_THIN:
                snapVlmData = layerDataFactory.createFileData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case LVM:
                snapVlmData = layerDataFactory.createLvmData(snapVlmRef, snapDataRef, storPool);
                break;
            case LVM_THIN:
                snapVlmData = layerDataFactory.createLvmThinData(snapVlmRef, snapDataRef, storPool);
                break;
            case STORAGE_SPACES:  // fall-through
            case STORAGE_SPACES_THIN:
                snapVlmData = layerDataFactory.createStorageSpacesData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case ZFS: // fall-through
            case ZFS_THIN:
                snapVlmData = layerDataFactory.createZfsData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case SPDK:
            case REMOTE_SPDK:
                snapVlmData = layerDataFactory.createSpdkData(snapVlmRef, snapDataRef, providerKind, storPool);
                break;
            case EBS_INIT:
            case EBS_TARGET:
                throw new ImplementationError("Snapshots are not supported for EBS-setups");
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            default:
                throw new ImplementationError("Unexpected kind: " + kind);
        }
        // To restore backups, the usable size is needed in the snapshot
        snapVlmData.setUsableSize(vlmLayerDataApiRef.getUsableSize());
        storPool.putSnapshotVolume(apiCtx, snapVlmData);
        return snapVlmData;
    }
}
