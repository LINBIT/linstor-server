package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public abstract class AbsLayerRscDataMerger<RSC extends AbsResource<RSC>>
{
    protected final AccessContext apiCtx;
    protected final LayerDataFactory layerDataFactory;

    public AbsLayerRscDataMerger(
        AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
    }

    private interface LayerRscDataMerger<RSC extends AbsResource<RSC>>
    {
        AbsRscLayerObject<RSC> mergeRscData(
            RSC rsc,
            RscLayerDataApi rscLayerDataPojo,
            AbsRscLayerObject<RSC> parent,
            boolean remoteResourceRef,
            List<RscLayerDataApi> pojoChildren
        )
            throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
                ExhaustedPoolException, ValueInUseException, InvalidNameException;
    }

    public void mergeLayerData(
        RSC rsc,
        RscLayerDataApi rscLayerDataPojo,
        boolean remoteResource
    )
    {
        try
        {
            merge(rsc, rscLayerDataPojo, null, remoteResource);
        }
        catch (AccessDeniedException | DatabaseException | ValueOutOfRangeException | IllegalArgumentException |
            ExhaustedPoolException | ValueInUseException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void merge(
        RSC rsc,
        RscLayerDataApi rscLayerDataPojo,
        @Nullable AbsRscLayerObject<RSC> parent,
        boolean remoteResourceRef
    )
        throws AccessDeniedException, DatabaseException, IllegalArgumentException,
            ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException, InvalidNameException
    {
        LayerRscDataMerger<RSC> rscMerger;
        switch (rscLayerDataPojo.getLayerKind())
        {
            case DRBD:
                rscMerger = this::mergeDrbdRscData;
                break;
            case LUKS:
                rscMerger = this::mergeLuksRscData;
                break;
            case STORAGE:
                rscMerger = this::mergeStorageRscData;
                break;
            case NVME:
                rscMerger = this::mergeNvmeRscData;
                break;
            case WRITECACHE:
                rscMerger = this::mergeWritecacheRscData;
                break;
            case CACHE:
                rscMerger = this::mergeCacheRscData;
                break;
            case BCACHE:
                rscMerger = this::mergeBCacheRscData;
                break;
            default:
                throw new ImplementationError("Unexpected layer kind: " + rscLayerDataPojo.getLayerKind());
        }
        AbsRscLayerObject<RSC> rscLayerObject = rscMerger.mergeRscData(
            rsc,
            rscLayerDataPojo,
            parent,
            remoteResourceRef,
            rscLayerDataPojo.getChildren()
        );
        // rscLayerObject.setSuspendIo(rscLayerDataPojo.getSuspend());

        for (RscLayerDataApi childRscPojo : rscLayerDataPojo.getChildren())
        {
            merge(rsc, childRscPojo, rscLayerObject, remoteResourceRef);
        }
    }

    private DrbdRscData<RSC> mergeDrbdRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> pojoChildren
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
            ExhaustedPoolException, ValueInUseException, InvalidNameException
    {
        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscDataPojo;

        DrbdRscDfnData<RSC> drbdRscDfnData = mergeOrCreateDrbdRscDfnData(
            rsc,
            drbdRscPojo.getDrbdRscDfn()
        );

        DrbdRscData<RSC> drbdRscData = null;
        if (parent == null)
        {
            drbdRscData = (DrbdRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            drbdRscData = findChild(parent, rscDataPojo.getId());
        }

        if (drbdRscData == null)
        {
            drbdRscData = createDrbdRscData(rsc, rscDataPojo, parent, drbdRscPojo, drbdRscDfnData);
        }
        else
        {
            mergeDrbdRscData(parent, drbdRscPojo, drbdRscData);
        }

        HashSet<VolumeNumber> drbdVlmDataToDelete = new HashSet<>(drbdRscData.getVlmLayerObjects().keySet());
        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (DrbdVlmPojo drbdVlmPojo : drbdRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(drbdVlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeDrbdVlm(drbdRscData, vlmNr);
            }
            else
            {
                drbdVlmDataToDelete.remove(vlmNr);
                restoreDrbdVlm(vlm, drbdRscData, drbdVlmPojo);
            }
        }
        for (VolumeNumber vlmNrToDelete : drbdVlmDataToDelete)
        {
            removeDrbdVlm(drbdRscData, vlmNrToDelete);
        }

        // it is possible (i.e. during a toggle disk) that we need to delete a child (i.e. ".meta") while keeping /
        // converting another child (i.e. from LVM/ZFS to Diskless)
        HashSet<Integer> childRscIdsToKeep = new HashSet<>();
        for (RscLayerDataApi pojoChild : pojoChildren)
        {
            childRscIdsToKeep.add(pojoChild.getId());
        }
        ArrayList<AbsRscLayerObject<RSC>> childrenToDelete = new ArrayList<>();
        for (AbsRscLayerObject<RSC> child : drbdRscData.getChildren())
        {
            if (!childRscIdsToKeep.contains(child.getRscLayerId()))
            {
                childrenToDelete.add(child);
            }
        }
        for (AbsRscLayerObject<RSC> childToDelete : childrenToDelete)
        {
            childToDelete.delete(apiCtx);
            drbdRscData.getChildren().remove(childToDelete);
        }

        return drbdRscData;
    }

    private void restoreDrbdVlm(
        AbsVolume<RSC> vlm,
        DrbdRscData<RSC> rscData,
        DrbdVlmPojo vlmPojo
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, InvalidNameException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        DrbdVlmDfnData<RSC> drbdVlmDfnData = mergeOrCreateDrbdVlmDfnData(vlm, vlmPojo.getDrbdVlmDfn());

        createOrMergeDrbdVlmData(vlm, rscData, vlmPojo, vlmNr, drbdVlmDfnData);
    }

    private LuksRscData<RSC> mergeLuksRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException
    {
        LuksRscPojo luksRscPojo = (LuksRscPojo) rscDataPojo;

        LuksRscData<RSC> luksRscData = null;
        if (parent == null)
        {
            luksRscData = (LuksRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            luksRscData = findChild(parent, rscDataPojo.getId());
        }

        if (luksRscData == null)
        {
            luksRscData = createLuksRscData(rsc, parent, luksRscPojo);
        }
        else
        {
            mergeLuksRscData(parent, luksRscPojo, luksRscData);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (LuksVlmPojo luksVlmPojo : luksRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(luksVlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeLuksVlm(luksRscData, vlmNr);
            }
            else
            {
                createOrMergeLuksVlm(vlm, luksRscData, luksVlmPojo);
            }
        }
        return luksRscData;
    }

    private StorageRscData<RSC> mergeStorageRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean remoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, InvalidNameException
    {
        StorageRscPojo storRscPojo = (StorageRscPojo) rscDataPojo;

        StorageRscData<RSC> storRscData = null;
        if (parent == null)
        {
            storRscData = (StorageRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            storRscData = findChild(parent, rscDataPojo.getId());
        }

        if (storRscData == null)
        {
            storRscData = createStorageRscData(rsc, parent, storRscPojo);
        }
        else
        {
            mergeStorageRscData(parent, storRscPojo, storRscData);
            updateParent(storRscData, parent);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (VlmLayerDataApi vlmPojo : storRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeStorageVlm(storRscData, vlmNr);
            }
            else
            {
                createOrMergeStorageVlm(vlm, storRscData, vlmPojo, remoteResource);
            }
        }
        return storRscData;
    }

    protected void createOrMergeStorageVlm(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        boolean remoteResourceRef
    )
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        StorPool storPool = getStoragePool(vlm, vlmPojo, remoteResourceRef);
        VlmProviderObject<RSC> vlmData = storRscData.getVlmLayerObjects().get(vlmNr);

        switch (vlmPojo.getProviderKind())
        {
            case DISKLESS:
                if (vlmData == null || !(vlmData instanceof DisklessData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createDisklessVlmData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeDisklessVlm(vlmPojo, vlmData);
                }
                break;
            case LVM:
                if (vlmData == null || !(vlmData instanceof LvmData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createLvmVlmData(vlm, storRscData, storPool);
                }
                else
                {
                    mergeLvmVlmData(vlmPojo, vlmData);
                }
                break;
            case LVM_THIN:
                if (vlmData == null || !(vlmData instanceof LvmThinData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createLvmThinVlmData(vlm, storRscData, storPool);
                }
                else
                {
                    mergeLvmThinVlmData(vlmPojo, vlmData);
                }
                break;
            case STORAGE_SPACES: // fall-through
            case STORAGE_SPACES_THIN:
                if (vlmData == null || !(vlmData instanceof StorageSpacesData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createStorageSpacesVlmData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeStorageSpacesVlmData(vlmPojo, vlmData);
                }
                break;
            case ZFS: // fall-through
            case ZFS_THIN:
                if (vlmData == null || !(vlmData instanceof ZfsData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createZfsData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeZfsData(vlmPojo, vlmData);
                }
                break;
            case FILE: // fall-through
            case FILE_THIN:
                if (vlmData == null || !(vlmData instanceof FileData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createFileData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeFileData(vlmPojo, vlmData);
                }
                break;
            case SPDK:
            case REMOTE_SPDK:
                if (vlmData == null || !(vlmData instanceof SpdkData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createSpdkVlmData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeSpdkVlmData(vlmPojo, vlmData);
                }
                break;
            case EXOS:
                if (vlmData == null || !(vlmData instanceof ExosData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createExosData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeExosData(vlmPojo, vlmData);
                }
                break;
            case EBS_INIT: // fall-through
            case EBS_TARGET:
                if (vlmData == null || !(vlmData instanceof EbsData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createEbsData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeEbsData(vlmPojo, vlmData);
                }
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected DeviceProviderKind: " + vlmPojo.getProviderKind());

        }

        // To restore backups, the usable size is needed in the snapshot
        if (vlmData.getUsableSize() == VlmProviderObject.UNINITIALIZED_SIZE)
        {
            vlmData.setUsableSize(vlmPojo.getUsableSize());
        }

        setStorPool(vlmData, storPool);
        putVlmData(storRscData, vlmData);
    }

    private NvmeRscData<RSC> mergeNvmeRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException
    {
        NvmeRscPojo nvmeRscPojo = (NvmeRscPojo) rscDataPojo;

        NvmeRscData<RSC> nvmeRscData = null;
        if (parent == null)
        {
            nvmeRscData = (NvmeRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            nvmeRscData = findChild(parent, rscDataPojo.getId());
        }

        if (nvmeRscData == null)
        {
            nvmeRscData = createNvmeRscData(rsc, parent, nvmeRscPojo);
        }
        else
        {
            mergeNvmeRscData(parent, nvmeRscPojo, nvmeRscData);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (NvmeVlmPojo vlmPojo : nvmeRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeNvmeVlm(nvmeRscData, vlmNr);
            }
            else
            {
                createOrMergeNvmeVlm(vlm, nvmeRscData, vlmPojo);
            }
        }
        return nvmeRscData;
    }

    private void createOrMergeNvmeVlm(AbsVolume<RSC> vlm, NvmeRscData<RSC> nvmeRscData, NvmeVlmPojo vlmPojo)
        throws DatabaseException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        NvmeVlmData<RSC> nvmeVlmData = nvmeRscData.getVlmLayerObjects().get(vlmNr);
        if (nvmeVlmData == null)
        {
            createNvmeVlm(vlm, nvmeRscData, vlmNr);
        }
        else
        {
            mergeNvmeVlm(vlmPojo, nvmeVlmData);
        }
    }

    private WritecacheRscData<RSC> mergeWritecacheRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, InvalidNameException
    {
        WritecacheRscPojo writecacheRscPojo = (WritecacheRscPojo) rscDataPojo;

        WritecacheRscData<RSC> writecacheRscData = null;
        if (parent == null)
        {
            writecacheRscData = (WritecacheRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            writecacheRscData = findChild(parent, rscDataPojo.getId());
        }

        if (writecacheRscData == null)
        {
            writecacheRscData = createWritecacheRscData(rsc, parent, writecacheRscPojo);
        }
        else
        {
            mergeWritecacheRscData(parent, writecacheRscPojo, writecacheRscData);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (WritecacheVlmPojo vlmPojo : writecacheRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeWritecacheVlm(writecacheRscData, vlmNr);
            }
            else
            {
                createOrMergeWritecacheVlm(vlm, writecacheRscData, vlmPojo);
            }
        }
        return writecacheRscData;
    }

    private void createOrMergeWritecacheVlm(
        AbsVolume<RSC> vlm,
        WritecacheRscData<RSC> writecacheRscData,
        WritecacheVlmPojo vlmPojo
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        WritecacheVlmData<RSC> writecacheVlmData = writecacheRscData.getVlmLayerObjects().get(vlmNr);
        if (writecacheVlmData == null)
        {
            createWritecacheVlm(vlm, writecacheRscData, vlmPojo, vlmNr);
        }
        else
        {
            mergeWritecacheVlm(vlmPojo, writecacheVlmData);
        }
    }

    private CacheRscData<RSC> mergeCacheRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, InvalidNameException
    {
        CacheRscPojo cacheRscPojo = (CacheRscPojo) rscDataPojo;

        CacheRscData<RSC> cacheRscData = null;
        if (parent == null)
        {
            cacheRscData = (CacheRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            cacheRscData = findChild(parent, rscDataPojo.getId());
        }

        if (cacheRscData == null)
        {
            cacheRscData = createCacheRscData(rsc, parent, cacheRscPojo);
        }
        else
        {
            mergeCacheRscData(parent, cacheRscPojo, cacheRscData);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (CacheVlmPojo vlmPojo : cacheRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeCacheVlm(cacheRscData, vlmNr);
            }
            else
            {
                createOrMergeCacheVlm(vlm, cacheRscData, vlmPojo);
            }
        }
        return cacheRscData;
    }

    private void createOrMergeCacheVlm(
        AbsVolume<RSC> vlm,
        CacheRscData<RSC> cacheRscData,
        CacheVlmPojo vlmPojo
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        CacheVlmData<RSC> cacheVlmData = cacheRscData.getVlmLayerObjects().get(vlmNr);
        if (cacheVlmData == null)
        {
            createCacheVlm(vlm, cacheRscData, vlmPojo, vlmNr);
        }
        else
        {
            mergeCacheVlm(vlmPojo, cacheVlmData);
        }
    }

    private BCacheRscData<RSC> mergeBCacheRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        boolean ignoredRemoteResource,
        List<RscLayerDataApi> ignoredPojoChildren
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, InvalidNameException
    {
        BCacheRscPojo bcacheRscPojo = (BCacheRscPojo) rscDataPojo;

        BCacheRscData<RSC> bcacheRscData = null;
        if (parent == null)
        {
            bcacheRscData = (BCacheRscData<RSC>) rsc.getLayerData(apiCtx);
        }
        else
        {
            bcacheRscData = findChild(parent, rscDataPojo.getId());
        }

        if (bcacheRscData == null)
        {
            bcacheRscData = createBCacheRscData(rsc, parent, bcacheRscPojo);
        }
        else
        {
            mergeBCacheRscData(parent, bcacheRscPojo, bcacheRscData);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (BCacheVlmPojo vlmPojo : bcacheRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeBCacheVlm(bcacheRscData, vlmNr);
            }
            else
            {
                createOrMergeBCacheVlm(vlm, bcacheRscData, vlmPojo);
            }
        }
        return bcacheRscData;
    }

    private void createOrMergeBCacheVlm(
        AbsVolume<RSC> vlm,
        BCacheRscData<RSC> bcacheRscData,
        BCacheVlmPojo vlmPojo
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        BCacheVlmData<RSC> bcacheVlmData = bcacheRscData.getVlmLayerObjects().get(vlmNr);
        if (bcacheVlmData == null)
        {
            createBCacheVlm(vlm, bcacheRscData, vlmPojo, vlmNr);
        }
        else
        {
            mergeBCacheVlm(vlmPojo, bcacheVlmData);
        }
    }

    protected @Nullable StorPool getStoragePool(
        AbsVolume<RSC> vlmRef,
        VlmLayerDataApi vlmPojoRef,
        boolean remoteResourceRef
    )
        throws InvalidNameException, AccessDeniedException
    {
        return vlmRef.getAbsResource().getNode().getStorPool(
            apiCtx,
            new StorPoolName(vlmPojoRef.getStorPoolApi().getStorPoolName())
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends AbsRscLayerObject<RSC>> @Nullable T findChild(AbsRscLayerObject<RSC> parent, int id)
    {
        AbsRscLayerObject<RSC> matchingChild = null;
        for (AbsRscLayerObject<RSC> child : parent.getChildren())
        {
            if (child.getRscLayerId() == id)
            {
                matchingChild = child;
                break;
            }
        }
        return (T) matchingChild;
    }



    /*
     * DRBD layer methods
     */
    protected abstract DrbdRscDfnData<RSC> mergeOrCreateDrbdRscDfnData(
        RSC rsc,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, AccessDeniedException, ValueOutOfRangeException;

    protected abstract DrbdRscData<RSC> createDrbdRscData(
        RSC rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<RSC> parent,
        DrbdRscPojo drbdRscPojo,
        DrbdRscDfnData<RSC> drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract void mergeDrbdRscData(
        AbsRscLayerObject<RSC> parent,
        DrbdRscPojo drbdRscPojo,
        DrbdRscData<RSC> drbdRscData
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, ImplementationError;

    protected abstract void removeDrbdVlm(DrbdRscData<RSC> drbdRscData, VolumeNumber vlmNr)
        throws AccessDeniedException, DatabaseException;

    protected abstract DrbdVlmDfnData<RSC> mergeOrCreateDrbdVlmDfnData(
        AbsVolume<RSC> absVlm,
        DrbdVlmDfnPojo drbdVlmDfnPojo
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException;

    protected abstract void createOrMergeDrbdVlmData(
        AbsVolume<RSC> vlm,
        DrbdRscData<RSC> rscData,
        DrbdVlmPojo vlmPojo,
        VolumeNumber vlmNr,
        DrbdVlmDfnData<RSC> drbdVlmDfnData
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;

    /*
     * LUKS layer methods
     */
    protected abstract LuksRscData<RSC> createLuksRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        LuksRscPojo luksRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeLuksRscData(
        AbsRscLayerObject<RSC> parent,
        LuksRscPojo luksRscPojo,
        LuksRscData<RSC> luksRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeLuksVlm(LuksRscData<RSC> luksRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createOrMergeLuksVlm(
        AbsVolume<RSC> vlm,
        LuksRscData<RSC> luksRscData,
        LuksVlmPojo vlmPojo
    )
        throws DatabaseException;

    /*
     * STORAGE layer methods
     */
    protected abstract StorageRscData<RSC> createStorageRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        StorageRscPojo storRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeStorageRscData(
        AbsRscLayerObject<RSC> parent,
        StorageRscPojo storRscPojo,
        StorageRscData<RSC> storRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeStorageVlm(StorageRscData<RSC> storRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract VlmProviderObject<RSC> createDisklessVlmData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeDisklessVlm(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createLvmVlmData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeLvmVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract LvmThinData<RSC> createLvmThinVlmData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createSpdkVlmData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createStorageSpacesVlmData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeStorageSpacesVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract void mergeSpdkVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract void mergeLvmThinVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createZfsData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeZfsData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createFileData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeFileData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract void setStorPool(VlmProviderObject<RSC> vlmDataRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException;

    protected abstract void putVlmData(StorageRscData<RSC> storRscDataRef, VlmProviderObject<RSC> vlmDataRef);

    @Deprecated(forRemoval = true)
    protected abstract VlmProviderObject<RSC> createExosData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException, AccessDeniedException;

    @Deprecated(forRemoval = true)
    protected abstract void mergeExosData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject<RSC> createEbsData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeEbsData(VlmLayerDataApi vlmPojo, VlmProviderObject<RSC> vlmData)
        throws DatabaseException;

    /*
     * NVME layer methods
     */
    protected abstract NvmeRscData<RSC> createNvmeRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        NvmeRscPojo nvmeRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeNvmeRscData(
        AbsRscLayerObject<RSC> parent,
        NvmeRscPojo nvmeRscPojo,
        NvmeRscData<RSC> nvmeRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeNvmeVlm(NvmeRscData<RSC> nvmeRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createNvmeVlm(AbsVolume<RSC> vlm, NvmeRscData<RSC> nvmeRscData, VolumeNumber vlmNr)
        throws DatabaseException;

    protected abstract void mergeNvmeVlm(NvmeVlmPojo vlmPojo, NvmeVlmData<RSC> nvmeVlmData) throws DatabaseException;

    /*
     * Writecache layer methods
     */
    protected abstract WritecacheRscData<RSC> createWritecacheRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        WritecacheRscPojo writecacheRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeWritecacheRscData(
        AbsRscLayerObject<RSC> parent,
        WritecacheRscPojo writecacheRscPojo,
        WritecacheRscData<RSC> writecacheRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeWritecacheVlm(WritecacheRscData<RSC> writecacheRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createWritecacheVlm(
        AbsVolume<RSC> vlm,
        WritecacheRscData<RSC> writecacheRscData,
        WritecacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNr
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;

    protected abstract void mergeWritecacheVlm(WritecacheVlmPojo vlmPojo, WritecacheVlmData<RSC> writecacheVlmData)
        throws DatabaseException;

    /*
     * Cache layer methods
     */
    protected abstract CacheRscData<RSC> createCacheRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        CacheRscPojo cacheRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeCacheRscData(
        AbsRscLayerObject<RSC> parent,
        CacheRscPojo cacheRscPojo,
        CacheRscData<RSC> cacheRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeCacheVlm(CacheRscData<RSC> cacheRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createCacheVlm(
        AbsVolume<RSC> vlm,
        CacheRscData<RSC> cacheRscData,
        CacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNr
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;

    protected abstract void mergeCacheVlm(CacheVlmPojo vlmPojo, CacheVlmData<RSC> cacheVlmData)
        throws DatabaseException;


    /*
     * BCache layer methods
     */
    protected abstract BCacheRscData<RSC> createBCacheRscData(
        RSC rsc,
        AbsRscLayerObject<RSC> parent,
        BCacheRscPojo bcacheRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeBCacheRscData(
        AbsRscLayerObject<RSC> parent,
        BCacheRscPojo bCacheRscPojo,
        BCacheRscData<RSC> bCacheRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeBCacheVlm(BCacheRscData<RSC> bcacheRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createBCacheVlm(
        AbsVolume<RSC> vlm,
        BCacheRscData<RSC> writecacheRscData,
        BCacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNr
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;

    protected abstract void mergeBCacheVlm(BCacheVlmPojo vlmPojo, BCacheVlmData<RSC> bcacheVlmData)
        throws DatabaseException;

    protected abstract void updateParent(AbsRscLayerObject<RSC> rscDataRef, AbsRscLayerObject<RSC> parentRef)
        throws DatabaseException;
}
