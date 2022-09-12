package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.StorageVlmPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.ExosMappingManager;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Singleton
class RscStorageLayerHelper extends AbsRscLayerHelper<
    StorageRscData<Resource>, VlmProviderObject<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final NameShortener exosNameShortener;
    private final ExosMappingManager exosMapMgr;

    @Inject
    RscStorageLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL)  DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        @Named(NameShortener.EXOS) NameShortener exosNameShortenerRef,
        ExosMappingManager exosMapMgrRef
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
        storPoolResolveHelper = storPoolResolveHelperRef;
        exosNameShortener = exosNameShortenerRef;
        exosMapMgr = exosMapMgrRef;
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
    protected Set<StorPool> getNeededStoragePools(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        Set<StorPool> neededStorPools = new HashSet<>();

        boolean resolveSp = false;

        AbsRscLayerObject<Resource> rscData = rsc.getLayerData(apiCtx);
        /*
         * If we are creating a (diskless?) resource, we might need to resolve storage pool or not.
         * If we are toggling disk we *must* resolve storage pools
         *
         * If we are creating a (diskless?) resource, we did not create StorRscData yet. (boolean will be false)
         * If we are toggling disk, we already have StorRscData from a previous run (boolean will be true)
         */
        boolean rscToggleDiskOrCreation = rscData != null &&
            !LayerRscUtils.getRscDataByProvider(rscData, DeviceLayerKind.STORAGE).isEmpty();

        for (StorageVlmPayload storageVlmPayload : payloadRef.storagePayload.values())
        {
            StorPool storPool = storageVlmPayload.storPool;
            if (storPool != null)
            {
                neededStorPools.add(storPool);
            }
            else
            {
                resolveSp = true;
            }
        }

        if (rscToggleDiskOrCreation || resolveSp)
        {
            CtrlRscLayerDataFactory ctrlRscLayerDataFactory = layerDataHelperProvider.get();
            StorPool resolvedStorPool = storPoolResolveHelper.resolveStorPool(
                apiCtx,
                rsc,
                vlmDfn,
                ctrlRscLayerDataFactory.isDiskless(rsc) && !ctrlRscLayerDataFactory.isDiskAddRequested(rsc),
                ctrlRscLayerDataFactory.isDiskRemoving(rsc),
                false
            ).extractApiCallRc(new ApiCallRcImpl());
            if (resolvedStorPool != null)
            {
                neededStorPools.add(resolvedStorPool);
            }
        }

        return neededStorPools;
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
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VlmProviderObject<Resource> vlmData = rscData.getVlmProviderObject(
            vlmDfn.getVolumeNumber()
        );
        if (vlmData == null)
        {
            switch (kind)
            {
                case DISKLESS:
                    vlmData = layerDataFactory.createDisklessData(
                        vlm,
                        vlmDfn.getVolumeSize(apiCtx),
                        rscData,
                        storPool
                    );
                    break;
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
                case REMOTE_SPDK:
                    if (rscData.getParent() == null || !rscData.getParent().getLayerKind().equals(DeviceLayerKind.NVME))
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_LAYER_STACK,
                                "SPDK storage requires NVME layer directly above"
                            )
                        );
                    }
                    vlmData = layerDataFactory.createSpdkData(vlm, rscData, kind, storPool);
                    break;
                case EXOS:
                    exosNameShortener.shorten(
                        vlmDfn,
                        storPool.getSharedStorPoolName().displayValue,
                        rscData.getResourceNameSuffix(),
                        false
                    );
                    try
                    {
                        exosMapMgr.findFreeExosPortAndLun(storPool, vlm);
                    }
                    catch (InvalidKeyException | InvalidValueException exc)
                    {
                        throw new ImplementationError(exc);
                    }

                    ExosData<Resource> exosData = layerDataFactory.createExosData(vlm, rscData, storPool);
                    exosData.updateShortName(apiCtx);
                    vlmData = exosData;
                    break;
                case OPENFLEX_TARGET:
                    throw new ImplementationError(
                        "Openflex volumes should be handled by openflex, not by storage helper"
                    );
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
                VolumeNumber vlmNr = vlmData.getVlmNr();
                // Remove the old data, which also ensures that createVlmLayerData doesn't just return the existing
                // object.
                storageRscData.remove(apiCtx, vlmNr);
                // if the kind changes, we basically need a new vlmData
                vlmData = createVlmLayerData(
                    storageRscData,
                    vlmRef,
                    payloadRef,
                    layerListRef
                );
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
    protected boolean recalculateVolatilePropertiesImpl(
        StorageRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        boolean changed = false;
        Resource rsc = rscDataRef.getAbsResource();

        Collection<VlmProviderObject<Resource>> vlmLayerObjects = rscDataRef.getVlmLayerObjects().values();

        // first run some checks depending on the storageProvider.
        {
            Set<DeviceProviderKind> providerKindSet = new HashSet<>();
            for (VlmProviderObject<Resource> vlmData : vlmLayerObjects)
            {
                providerKindSet.add(vlmData.getProviderKind());
            }

            String reason = IGNORE_REASON_NONE;
            if (providerKindSet.contains(DeviceProviderKind.OPENFLEX_TARGET))
            {
                reason = IGNORE_REASON_OF_TARGET;
            }
            else if (providerKindSet.contains(DeviceProviderKind.SPDK) ||
                providerKindSet.contains(DeviceProviderKind.REMOTE_SPDK))
            {
                reason = IGNORE_REASON_SPDK_TARGET;
            }

            if (reason != null) // IGNORE_REASON_NONE == null
            {
                changed |= setIgnoreReason(rscDataRef, reason, true, false, true);
            }
        }

        StateFlags<Flags> rscFlags = rsc.getStateFlags();
        if (rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE) && rscFlags.isUnset(apiCtx, Resource.Flags.INACTIVATING))
        {
            // do not propagate the reason while we are still inactivating the resource.
            changed |= setIgnoreReason(rscDataRef, IGNORE_REASON_RSC_INACTIVE, true, false, true);
        }
        if (rsc.streamVolumes().anyMatch(
            vlm -> isAnyVolumeFlagSetPrivileged(vlm, Volume.Flags.CLONING, Volume.Flags.CLONING_START) &&
                !areAllVolumeFlagsSetPrivileged(vlm, Volume.Flags.CLONING_FINISHED)
        ))
        {
            changed |= setIgnoreReason(rscDataRef, IGNORE_REASON_RSC_CLONING, true, false, true);
        }
        return changed;
    }

    private boolean areAllVolumeFlagsSetPrivileged(Volume vlm, Volume.Flags... flags)
    {
        boolean isSet = false;
        try
        {
            isSet = vlm.getFlags().isSet(apiCtx, flags);
        }
        catch (AccessDeniedException ignored)
        {
        }
        return isSet;
    }

    private boolean isAnyVolumeFlagSetPrivileged(Volume vlm, Volume.Flags... flags)
    {
        boolean isSet = false;
        try
        {
            isSet = vlm.getFlags().isSomeSet(apiCtx, flags);
        }
        catch (AccessDeniedException ignored)
        {
        }
        return isSet;
    }

    @Override
    protected boolean isExpectedToProvideDevice(StorageRscData<Resource> storageRscData) throws AccessDeniedException
    {
        return storageRscData.getIgnoreReason() != null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    )
    {
        // StorageLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromSnapVlmDataRef
    )
    {
        // StorageLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> StorageRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createStorageRscData(
            layerRscIdPool.autoAllocate(),
            rscParentRef,
            rscRef,
            fromAbsRscDataRef.getResourceNameSuffix()
        );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> VlmProviderObject<Resource> restoreVlmData(
        Volume vlmRef,
        StorageRscData<Resource> storRscData,
        VlmProviderObject<RSC> snapVlmData
    )
        throws DatabaseException, AccessDeniedException, LinStorException
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
            case REMOTE_SPDK:
                vlmData = layerDataFactory.createSpdkData(vlmRef, storRscData, providerKind, storPool);
                break;
            case EXOS:
                exosNameShortener.shorten(
                    vlmRef.getVolumeDefinition(),
                    storPool.getSharedStorPoolName().displayValue,
                    storRscData.getResourceNameSuffix(),
                    false
                );
                ExosData<Resource> exosData = layerDataFactory.createExosData(vlmRef, storRscData, storPool);
                exosData.updateShortName(apiCtx);
                vlmData = exosData;
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected kind: " + kind);
        }
        storPool.putVolume(apiCtx, vlmData);
        return vlmData;
    }
}
