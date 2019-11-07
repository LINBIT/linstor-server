package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdRscDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.api.pojo.StorageRscPojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.LayerDataFactory;

public abstract class AbsLayerRscDataMerger
{
    protected final AccessContext apiCtx;
    protected final LayerDataFactory layerDataFactory;

    public AbsLayerRscDataMerger(
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
    }

    private interface LayerRscDataMerger
    {
        RscLayerObject mergeRscData(
            Resource rsc,
            RscLayerDataApi rscLayerDataPojo,
            RscLayerObject parent,
            boolean remoteResourceRef
        )
            throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
                ExhaustedPoolException, ValueInUseException, InvalidNameException;
    }

    public void mergeLayerData(
        Resource rsc,
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
        Resource rsc,
        RscLayerDataApi rscLayerDataPojo,
        RscLayerObject parent,
        boolean remoteResourceRef
    )
        throws AccessDeniedException, DatabaseException, IllegalArgumentException,
            ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException, InvalidNameException
    {
        LayerRscDataMerger rscMerger;
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
            default:
                throw new ImplementationError("Unexpected layer kind: " + rscLayerDataPojo.getLayerKind());
        }
        RscLayerObject rscLayerObject = rscMerger.mergeRscData(rsc, rscLayerDataPojo, parent, remoteResourceRef);

        for (RscLayerDataApi childRscPojo : rscLayerDataPojo.getChildren())
        {
            merge(rsc, childRscPojo, rscLayerObject, remoteResourceRef);
        }
    }

    private DrbdRscData mergeDrbdRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent,
        boolean ignoredRemoteResource
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, IllegalArgumentException,
            ExhaustedPoolException, ValueInUseException, InvalidNameException
    {
        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rscDataPojo;

        DrbdRscDfnData drbdRscDfnData = mergeOrCreateDrbdRscDfnData(
            rsc.getDefinition(),
            drbdRscPojo.getDrbdRscDfn()
        );

        DrbdRscData drbdRscData = null;
        if (parent == null)
        {
            drbdRscData = (DrbdRscData) rsc.getLayerData(apiCtx);
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

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (DrbdVlmPojo drbdVlmPojo : drbdRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(drbdVlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
            if (vlm == null)
            {
                removeDrbdVlm(drbdRscData, vlmNr);
            }
            else
            {
                restoreDrbdVlm(vlm, drbdRscData, drbdVlmPojo);
            }
        }
        return drbdRscData;
    }

    private void restoreDrbdVlm(Volume vlm, DrbdRscData rscData, DrbdVlmPojo vlmPojo)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, InvalidNameException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        DrbdVlmDfnData drbdVlmDfnData = mergeOrCreateDrbdVlmDfnData(vlmDfn, vlmPojo.getDrbdVlmDfn());

        createOrMergeDrbdVlmData(vlm, rscData, vlmPojo, vlmNr, drbdVlmDfnData);
    }

    private LuksRscData mergeLuksRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent,
        boolean ignoredRemoteResource
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException
    {
        LuksRscPojo luksRscPojo = (LuksRscPojo) rscDataPojo;

        LuksRscData luksRscData = null;
        if (parent == null)
        {
            luksRscData = (LuksRscData) rsc.getLayerData(apiCtx);
        }
        else
        {
            luksRscData = findChild(parent, rscDataPojo.getId());
        }

        if (luksRscData == null)
        {
            luksRscData = createLuksRscData(rsc, parent, luksRscPojo);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (LuksVlmPojo luksVlmPojo : luksRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(luksVlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
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

    private StorageRscData mergeStorageRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent,
        boolean remoteResource
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, InvalidNameException
    {
        StorageRscPojo storRscPojo = (StorageRscPojo) rscDataPojo;
        StorageRscData storRscData = null;
        if (parent == null)
        {
            storRscData = (StorageRscData) rsc.getLayerData(apiCtx);
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
            updateParent(storRscData, parent);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (VlmLayerDataApi vlmPojo : storRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
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
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        boolean remoteResourceRef
    )
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        StorPool storPool = getStoragePool(vlm, vlmPojo, remoteResourceRef);
        VlmProviderObject vlmData = storRscData.getVlmLayerObjects().get(vlmNr);

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
            case SWORDFISH_INITIATOR:
                if (vlmData == null || !(vlmData instanceof SfInitiatorData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createSfInitVlmData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeSfInitVlmData(vlmPojo, vlmData);
                }
                break;
            case SWORDFISH_TARGET:
                if (vlmData == null || !(vlmData instanceof SfTargetData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createSfTargetVlmData(vlm, storRscData, vlmPojo, storPool);
                }
                else
                {
                    mergeSfTargetVlmData(vlmPojo, vlmData);
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
                if (vlmData == null || !(vlmData instanceof SpdkData))
                {
                    if (vlmData != null)
                    {
                        removeStorageVlm(storRscData, vlmNr);
                    }
                    vlmData = createSpdkVlmData(vlm, storRscData, storPool);
                }
                else
                {
                    mergeSpdkVlmData(vlmPojo, vlmData);
                }
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected DeviceProviderKind: " + vlmPojo.getProviderKind());

        }

        setStorPool(vlmData, storPool);
        putVlmData(storRscData, vlmData);
    }

    private NvmeRscData mergeNvmeRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        RscLayerObject parent,
        boolean ignoredRemoteResource
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException
    {
        NvmeRscPojo nvmeRscPojo = (NvmeRscPojo) rscDataPojo;

        NvmeRscData nvmeRscData = null;
        if (parent == null)
        {
            nvmeRscData = (NvmeRscData) rsc.getLayerData(apiCtx);
        }
        else
        {
            nvmeRscData = findChild(parent, rscDataPojo.getId());
        }

        if (nvmeRscData == null)
        {
            nvmeRscData = createNvmeRscData(rsc, parent, nvmeRscPojo);
        }

        // do not iterate over rsc.volumes as those might have changed in the meantime
        // see gitlab 368
        for (NvmeVlmPojo vlmPojo : nvmeRscPojo.getVolumeList())
        {
            VolumeNumber vlmNr = new VolumeNumber(vlmPojo.getVlmNr());
            Volume vlm = rsc.getVolume(vlmNr);
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

    private void createOrMergeNvmeVlm(Volume vlm, NvmeRscData nvmeRscData, NvmeVlmPojo vlmPojo)
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        NvmeVlmData nvmeVlmData = nvmeRscData.getVlmLayerObjects().get(vlmNr);
        if (nvmeVlmData == null)
        {
            createNvmeVlm(vlm, nvmeRscData, vlmNr);
        }
        else
        {
            mergeNvmeVlm(vlmPojo, nvmeVlmData);
        }
    }

    protected StorPool getStoragePool(Volume vlmRef, VlmLayerDataApi vlmPojoRef, boolean remoteResourceRef)
        throws InvalidNameException, AccessDeniedException
    {
        return vlmRef.getResource().getAssignedNode().getStorPool(
            apiCtx,
            new StorPoolName(vlmPojoRef.getStorPoolApi().getStorPoolName())
        );
    }

    @SuppressWarnings("unchecked")
    private <T extends RscLayerObject> T findChild(RscLayerObject parent, int id)
    {
        RscLayerObject matchingChild = null;
        for (RscLayerObject child : parent.getChildren())
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
    protected abstract DrbdRscDfnData mergeOrCreateDrbdRscDfnData(
        ResourceDefinition rscDfn,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
            ExhaustedPoolException, ValueInUseException;

    protected abstract DrbdRscData createDrbdRscData(
        Resource rsc, RscLayerDataApi rscDataPojo, RscLayerObject parent, DrbdRscPojo drbdRscPojo,
        DrbdRscDfnData drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException;

    protected abstract void mergeDrbdRscData(
        RscLayerObject parent,
        DrbdRscPojo drbdRscPojo,
        DrbdRscData drbdRscData
    )
        throws AccessDeniedException, DatabaseException;

    protected abstract void removeDrbdVlm(DrbdRscData drbdRscData, VolumeNumber vlmNr)
        throws AccessDeniedException, DatabaseException;

    protected abstract DrbdVlmDfnData mergeOrCreateDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        DrbdVlmDfnPojo drbdVlmDfnPojo
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException;

    protected abstract void createOrMergeDrbdVlmData(
        Volume vlm, DrbdRscData rscData, DrbdVlmPojo vlmPojo, VolumeNumber vlmNr, DrbdVlmDfnData drbdVlmDfnData
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;

    /*
     * LUKS layer methods
     */
    protected abstract LuksRscData createLuksRscData(Resource rsc, RscLayerObject parent, LuksRscPojo luksRscPojo)
        throws DatabaseException, AccessDeniedException;

    protected abstract void removeLuksVlm(LuksRscData luksRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createOrMergeLuksVlm(
        Volume vlm,
        LuksRscData luksRscData,
        LuksVlmPojo vlmPojo
    )
        throws DatabaseException;

    /*
     * STORAGE layer methods
     */
    protected abstract StorageRscData createStorageRscData(Resource rsc, RscLayerObject parent, StorageRscPojo storRscPojo)
        throws DatabaseException, AccessDeniedException;

    protected abstract void removeStorageVlm(StorageRscData storRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract VlmProviderObject createDisklessVlmData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeDisklessVlm(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract VlmProviderObject createLvmVlmData(Volume vlm, StorageRscData storRscData, StorPool storPool)
        throws DatabaseException;

    protected abstract void mergeLvmVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract LvmThinData createLvmThinVlmData(Volume vlm, StorageRscData storRscData, StorPool storPool)
        throws DatabaseException;

    protected abstract VlmProviderObject createSpdkVlmData(Volume vlm, StorageRscData storRscData, StorPool storPool)
            throws DatabaseException;

    protected abstract void mergeSpdkVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract void mergeLvmThinVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject createSfInitVlmData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeSfInitVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData)
        throws DatabaseException;

    protected abstract VlmProviderObject createSfTargetVlmData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void mergeSfTargetVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract VlmProviderObject createZfsData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeZfsData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract VlmProviderObject createFileData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmPojo,
        StorPool storPool
    )
        throws DatabaseException;

    protected abstract void mergeFileData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException;

    protected abstract void setStorPool(VlmProviderObject vlmDataRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException;

    protected abstract void putVlmData(StorageRscData storRscDataRef, VlmProviderObject vlmDataRef);


    /*
     * NVME layer methods
     */

    protected abstract NvmeRscData createNvmeRscData(
        Resource rsc,
        RscLayerObject parent,
        NvmeRscPojo nvmeRscPojo
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract void removeNvmeVlm(NvmeRscData nvmeRscData, VolumeNumber vlmNr)
        throws DatabaseException, AccessDeniedException;

    protected abstract void createNvmeVlm(Volume vlm, NvmeRscData nvmeRscData, VolumeNumber vlmNr);

    protected abstract void mergeNvmeVlm(NvmeVlmPojo vlmPojo, NvmeVlmData nvmeVlmData);



    protected abstract void updateParent(RscLayerObject rscDataRef, RscLayerObject parentRef) throws DatabaseException;
}
