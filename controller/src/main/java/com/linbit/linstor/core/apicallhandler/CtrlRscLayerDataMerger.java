package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
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
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;
import com.linbit.utils.TimedCache;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

@Singleton
public class CtrlRscLayerDataMerger extends AbsLayerRscDataMerger<Resource>
{
    // entries are kept for one hour
    private final TimedCache<Integer, Optional<Integer>> replacedLayerIdCache = new TimedCache<>(1 * 60 * 60 * 1000L);

    @Inject
    public CtrlRscLayerDataMerger(
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        super(apiCtxRef, layerDataFactoryRef);
    }

    @Override
    protected DrbdRscDfnData<Resource> mergeOrCreateDrbdRscDfnData(
        Resource rsc,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, AccessDeniedException
    {
        // nothing to merge
        return rsc.getResourceDefinition().getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdRscDfnPojo.getRscNameSuffix()
        );
    }

    @Override
    protected DrbdRscData<Resource> createDrbdRscData(
        Resource rscRef,
        RscLayerDataApi rscDataPojoRef,
        AbsRscLayerObject<Resource> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscDfnData<Resource> drbdRscDfnDataRef
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown drbd resource from satellite");
    }

    @Override
    protected void mergeDrbdRscData(
        AbsRscLayerObject<Resource> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscData<Resource> drbdRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData<Resource> drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected DrbdVlmDfnData<Resource> mergeOrCreateDrbdVlmDfnData(
        AbsVolume<Resource> absVlm,
        DrbdVlmDfnPojo drbdVlmDfnPojoRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // nothing to merge
        VolumeDefinition vlmDfn = absVlm.getVolumeDefinition();
        return vlmDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD, drbdVlmDfnPojoRef.getRscNameSuffix());
    }

    @Override
    protected void createOrMergeDrbdVlmData(
        AbsVolume<Resource> vlmRef,
        DrbdRscData<Resource> rscDataRef,
        DrbdVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef,
        DrbdVlmDfnData<Resource> drbdVlmDfnDataRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        DrbdVlmData<Resource> drbdVlmData = rscDataRef.getVlmLayerObjects().get(vlmNrRef);

        drbdVlmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        drbdVlmData.setDevicePath(vlmPojoRef.getDevicePath());
        drbdVlmData.setDiskState(vlmPojoRef.getDiskState());
        drbdVlmData.setUsableSize(vlmPojoRef.getUsableSize());
        drbdVlmData.setDiscGran(vlmPojoRef.getDiscGran());
        // ignore externalMetaDataStorPool
    }

    @Override
    protected LuksRscData<Resource> createLuksRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        LuksRscPojo luksRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown luks resource from satellite");
    }

    @Override
    protected void mergeLuksRscData(
        AbsRscLayerObject<Resource> parentRef,
        LuksRscPojo luksRscPojoRef,
        LuksRscData<Resource> luksRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeLuksVlm(LuksRscData<Resource> luksRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createOrMergeLuksVlm(
        AbsVolume<Resource> vlmRef,
        LuksRscData<Resource> luksRscDataRef,
        LuksVlmPojo vlmPojoRef
    )
        throws DatabaseException
    {
        LuksVlmData<Resource> luksVlmData = luksRscDataRef.getVlmLayerObjects().get(
            vlmRef.getVolumeDefinition().getVolumeNumber()
        );
        luksVlmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        luksVlmData.setDataDevice(vlmPojoRef.getDataDevice());
        luksVlmData.setDevicePath(vlmPojoRef.getDevicePath());
        luksVlmData.setOpened(vlmPojoRef.isOpen());
        luksVlmData.setDiskState(vlmPojoRef.getDiskState());
        luksVlmData.setUsableSize(vlmPojoRef.getUsableSize());
        luksVlmData.setDiscGran(vlmPojoRef.getDiscGran());
        luksVlmData.setEncryptedKey(vlmPojoRef.getEncryptedPassword());
        luksVlmData.setModifyPassword(vlmPojoRef.getModifyPassword());
    }

    @Override
    protected StorageRscData<Resource> createStorageRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        StorageRscPojo storRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown storage resource from satellite");
    }

    @Override
    protected void mergeStorageRscData(
        AbsRscLayerObject<Resource> parentRef,
        StorageRscPojo storRscPojoRef,
        StorageRscData<Resource> storRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeStorageVlm(StorageRscData<Resource> storRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void updateParent(
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Resource> parentRef
    )
        throws DatabaseException
    {
        // ignored
    }

    @Override
    protected VlmProviderObject<Resource> createDisklessVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown diskless storage volume from satellite");
    }

    @Override
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        DisklessData<Resource> disklessData = (DisklessData<Resource>) vlmDataRef;
        disklessData.setUsableSize(vlmPojoRef.getUsableSize());
        disklessData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected VlmProviderObject<Resource> createLvmVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm storage volume from satellite");
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        LvmData<Resource> lvmData = (LvmData<Resource>) vlmDataRef;
        lvmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        lvmData.setDevicePath(vlmPojoRef.getDevicePath());
        lvmData.setUsableSize(vlmPojoRef.getUsableSize());
        lvmData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected VlmProviderObject<Resource> createStorageSpacesVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown Storage Spaces storage volume from satellite");
    }

    @Override
    protected void mergeStorageSpacesVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        StorageSpacesData<Resource> storageSpacesData = (StorageSpacesData<Resource>) vlmDataRef;
        storageSpacesData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        storageSpacesData.setDevicePath(vlmPojoRef.getDevicePath());
        storageSpacesData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected VlmProviderObject<Resource> createSpdkVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError(
            "Received unknown " + vlmPojoRef.getProviderKind() + " storage volume from satellite"
        );
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        SpdkData<Resource> spdkData = (SpdkData<Resource>) vlmDataRef;
        spdkData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        spdkData.setDevicePath(vlmPojoRef.getDevicePath());
        spdkData.setUsableSize(vlmPojoRef.getUsableSize());
        spdkData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected LvmThinData<Resource> createLvmThinVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm thin storage volume from satellite");
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        LvmThinData<Resource> lvmThinData = (LvmThinData<Resource>) vlmDataRef;
        lvmThinData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        lvmThinData.setDevicePath(vlmPojoRef.getDevicePath());
        lvmThinData.setUsableSize(vlmPojoRef.getUsableSize());
        lvmThinData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected VlmProviderObject<Resource> createZfsData(
        AbsVolume<Resource> vlm,
        StorageRscData<Resource> storRscData,
        VlmLayerDataApi vlmDataApi,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown zfs storage volume from satellite");
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        ZfsData<Resource> zfsData = (ZfsData<Resource>) vlmDataRef;
        zfsData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        zfsData.setDevicePath(vlmPojoRef.getDevicePath());
        zfsData.setUsableSize(vlmPojoRef.getUsableSize());
        zfsData.setDiscGran(vlmPojoRef.getDiscGran());
        zfsData.setExtentSize(vlmPojoRef.getExtentSize());
    }

    @Override
    protected VlmProviderObject<Resource> createFileData(
        AbsVolume<Resource> vlm,
        StorageRscData<Resource> storRsc,
        VlmLayerDataApi vlmDataApi,
        StorPool storPool
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown file storage volume from satellite");
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        FileData<Resource> fileData = (FileData<Resource>) vlmDataRef;
        fileData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        fileData.setDevicePath(vlmPojoRef.getDevicePath());
        fileData.setUsableSize(vlmPojoRef.getUsableSize());
        fileData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected void setStorPool(VlmProviderObject<Resource> vlmDataRef, StorPool storPoolRef)
    {
        // ignored
    }

    @Override
    protected void putVlmData(
        StorageRscData<Resource> storRscDataRef,
        VlmProviderObject<Resource> vlmDataRef
    )
    {
        // ignored
    }

    @Override
    protected NvmeRscData<Resource> createNvmeRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        NvmeRscPojo nvmeRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown nvme resource from satellite");
    }

    @Override
    protected void mergeNvmeRscData(
        AbsRscLayerObject<Resource> parentRef,
        NvmeRscPojo nvmeRscPojoRef,
        NvmeRscData<Resource> nvmeRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void createNvmeVlm(
        AbsVolume<Resource> vlmRef,
        NvmeRscData<Resource> nvmeRscDataRef,
        VolumeNumber vlmNrRef
    )
    {
        throw new ImplementationError("Missing nvme volume from satellite");
    }

    @Override
    protected void removeNvmeVlm(NvmeRscData<Resource> nvmeRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData<Resource> nvmeVlmDataRef) throws DatabaseException
    {
        nvmeVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        nvmeVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        nvmeVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        nvmeVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
        nvmeVlmDataRef.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected WritecacheRscData<Resource> createWritecacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        WritecacheRscPojo writecacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown writecache resource from satellite");
    }

    @Override
    protected void mergeWritecacheRscData(
        AbsRscLayerObject<Resource> parent,
        WritecacheRscPojo writecacheRscPojo,
        WritecacheRscData<Resource> writecacheRscData)
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void createWritecacheVlm(
        AbsVolume<Resource> vlmRef,
        WritecacheRscData<Resource> writecacheRscDataRef,
        WritecacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    )
    {
        throw new ImplementationError("Missing writecache volume from satellite");
    }

    @Override
    protected void removeWritecacheVlm(WritecacheRscData<Resource> writecacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void mergeWritecacheVlm(WritecacheVlmPojo vlmPojoRef, WritecacheVlmData<Resource> writecacheVlmDataRef)
        throws DatabaseException
    {
        writecacheVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        writecacheVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        writecacheVlmDataRef.setDataDevice(vlmPojoRef.getDataDevice());
        writecacheVlmDataRef.setCacheDevice(vlmPojoRef.getCacheDevice());
        writecacheVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        writecacheVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
        writecacheVlmDataRef.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected CacheRscData<Resource> createCacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        CacheRscPojo writecacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown cache resource from satellite");
    }

    @Override
    protected void mergeCacheRscData(
        AbsRscLayerObject<Resource> parentRef,
        CacheRscPojo cacheRscPojoRef,
        CacheRscData<Resource> cacheRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void createCacheVlm(
        AbsVolume<Resource> vlmRef,
        CacheRscData<Resource> writecacheRscDataRef,
        CacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    )
    {
        throw new ImplementationError("Missing cache volume from satellite");
    }

    @Override
    protected void removeCacheVlm(CacheRscData<Resource> cacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void mergeCacheVlm(CacheVlmPojo vlmPojoRef, CacheVlmData<Resource> cacheVlmDataRef)
        throws DatabaseException
    {
        cacheVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        cacheVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        cacheVlmDataRef.setDataDevice(vlmPojoRef.getDataDevice());
        cacheVlmDataRef.setCacheDevice(vlmPojoRef.getCacheDevice());
        cacheVlmDataRef.setMetaDevice(vlmPojoRef.getMetaDevice());
        cacheVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        cacheVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
        cacheVlmDataRef.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected BCacheRscData<Resource> createBCacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        BCacheRscPojo bcacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown bcache resource from satellite");
    }

    @Override
    protected void mergeBCacheRscData(
        AbsRscLayerObject<Resource> parent,
        BCacheRscPojo bCacheRscPojo,
        BCacheRscData<Resource> bCacheRscData)
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void createBCacheVlm(
        AbsVolume<Resource> vlmRef,
        BCacheRscData<Resource> bcacheRscDataRef,
        BCacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    )
    {
        throw new ImplementationError("Missing bcache volume from satellite");
    }

    @Override
    protected void removeBCacheVlm(BCacheRscData<Resource> bcacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void mergeBCacheVlm(BCacheVlmPojo vlmPojoRef, BCacheVlmData<Resource> bcacheVlmDataRef)
        throws DatabaseException
    {
        bcacheVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        bcacheVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        bcacheVlmDataRef.setDataDevice(vlmPojoRef.getDataDevice());
        bcacheVlmDataRef.setCacheDevice(vlmPojoRef.getCacheDevice());
        bcacheVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        bcacheVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
        bcacheVlmDataRef.setDeviceUuid(vlmPojoRef.getDeviceUuid());
        bcacheVlmDataRef.setDiscGran(vlmPojoRef.getDiscGran());
    }

    @Override
    protected VlmProviderObject<Resource> createEbsData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown EBS resource from satellite");
    }

    @Override
    protected void mergeEbsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        EbsData<Resource> ebsData = (EbsData<Resource>) vlmDataRef;
        ebsData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        ebsData.setDevicePath(vlmPojoRef.getDevicePath());
        ebsData.setUsableSize(vlmPojoRef.getUsableSize());
        ebsData.setDiscGran(vlmPojoRef.getDiscGran());
    }

    /**
     * There is an inherent race condition during a toggle disk. Imagine the following scenario:
     * <ul>
     *  <li>Setup:
     *   <ul>
     *    <li>We have a resource with DrbdRscData and a StorageRscData as a child. Let us assume that this
     *        StorageRscData has an LvmData as a volume</li>
     *    <li>We are about to toggle disk this resource into a diskless. That will eventually cause the DrbdRscData
     *        as well as the StorageRscData to be replaced with a new DrbdRscData and a new StorageRscData but this
     *        time the latter will have a DisklessData as a volume.</li>
     *   </ul>
     *  </li>
     *  <li>Scenario:
     *   <ul>
     *    <li>We are skipping the first few "updateSatellite" calls where the satellites are prepared for the toggle-
     *        disk event.</li>
     *    <li>The controller just sent out the "updateSatellite" that is the very last update that still contains
     *        the LvmData. Let us call this "update1" for now. In a usual scenario once the satellite answers
     *        the controller will throw away the old layer data of the resource and replace it with the newly created
     *        layer data that contains a DisklessData instead of the LvmData. But we are not there yet in this
     *        scenario.</li>
     *    <li>While the controller is waiting for the satellite's response (to "update1"), an asynchronous event
     *        (second client or another LINSTOR internal task, or something else) runs an operation that also requires
     *        another "updateSatellite". Let us call this "update2".</li>
     *    <li>The controller will eventually receive the expected response for "update1". Such a response from the
     *        satellite also contains the layer data the satellite used for the just applied operation. That
     *        means in our case that this layer data has a reference to the StorageRscData containing the
     *        LvmData. This is expected. The controller now performs the next step of the toggle disk scenario which
     *        is to replace the old layer data with a new one containing a DisklessVlm and sends out another
     *        "updateSatellite" (we could call this "update3" but we do not care about this update since an the error
     *        we describe here will occur before we could receive a response to "update3").</li>
     *    <li>Eventually the controller will also receive the response for "update2". Just as before the "update2"
     *        also contains the layer data the satellite operated on. In this case it is still the old StorageRscData
     *        with the LvmData. The controller however no longer knows about this layer data. This is how the
     *        {@link #createStorageRscData(Resource, AbsRscLayerObject, StorageRscPojo)} gets called with the old
     *        ID (which is not found) and throws an "Received unknown storage resource from satellite"
     *        ImplementationError.</li>
     *   </ul>
     *  </li>
     *  <li>Solution:<br/>
     *      When the controller is throwing away the old layer data it has to remember the old LayerRscId and call
     *      {@link #replacedLayerRscId(Integer, int)} with those two IDs. This replacement is remembered for a given
     *      time and when a satellite response is referring to an old ID, we can use this method her to detect this
     *      scenario and conclude that it is safe to ignore this update for now since there should also be a second
     *      "updateSatellite" in flight that will be responded using the correct layer data.
     *  </li>
     * </ul>
     * @param rscLayerObjectRef
     * @param rscDataPojoRef
     * @return Iff we are aware that we just threw away some old RscLayerData but a satellite might still refer to that.
     */
    @Override
    protected boolean wasRscLayerDataRecentlyReplaced(
        AbsRscLayerObject<Resource> rscDataRef,
        RscLayerDataApi rscDataPojoRef
    )
    {
        boolean ret = false;
        @Nullable Optional<Integer> optReplacedByNewId;
        int oldId = rscDataPojoRef.getId();
        optReplacedByNewId = replacedLayerIdCache.get(oldId);
        if (optReplacedByNewId != null)
        {
            if (optReplacedByNewId.isEmpty())
            {
                ret = true;
            }
            else
            {
                int replacedByNewId = optReplacedByNewId.get();
                ret = replacedByNewId == rscDataRef.getRscLayerId();
            }
        }
        return ret;
    }

    /**
     * Needs to be called if a RscLayerData is replaced with new RscLayerData for the scenario described in
     * {@link #wasRscLayerDataRecentlyReplaced(AbsRscLayerObject, RscLayerDataApi)}
     *
     * @param oldLayerRscIdRef The old LayerRscId that was/is being deleted
     * @param layerRscIdRef The new LayerRscId if known. If an old layer-data was replaced with an entirely different
     *   layer-structure such that no one to one mapping between old and new layerRscId is possible, just pass
     *   <code>null</code> here and all <code>oldLayerRscId</code> is ignored, regardless what the new LayerRscId
     *   is.
     */
    @SuppressWarnings("javadoc")
    public void replacedLayerRscId(int oldLayerRscIdRef, @Nullable Integer layerRscIdRef)
    {
        replacedLayerIdCache.put(oldLayerRscIdRef, Optional.ofNullable(layerRscIdRef));
    }
}
