package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
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
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
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
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;

public class CtrlSnapLayerDataMerger extends AbsLayerRscDataMerger<Snapshot>
{
    @Inject
    public CtrlSnapLayerDataMerger(
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        super(apiCtxRef, layerDataFactoryRef);
    }

    @Override
    protected DrbdRscDfnData<Snapshot> mergeOrCreateDrbdRscDfnData(Snapshot snap, DrbdRscDfnPojo drbdRscDfnPojoRef)
        throws IllegalArgumentException, DatabaseException, AccessDeniedException
    {
        // nothing to merge
        return snap.getSnapshotDefinition()
            .getLayerData(
                apiCtx,
                DeviceLayerKind.DRBD,
                drbdRscDfnPojoRef.getRscNameSuffix()
            );
    }

    @Override
    protected DrbdRscData<Snapshot> createDrbdRscData(
        Snapshot rscRef,
        RscLayerDataApi rscDataPojoRef,
        AbsRscLayerObject<Snapshot> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscDfnData<Snapshot> drbdRscDfnDataRef
    ) throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown drbd snapshot from satellite");
    }

    @Override
    protected void mergeDrbdRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscData<Snapshot> drbdRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData<Snapshot> drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected DrbdVlmDfnData<Snapshot> mergeOrCreateDrbdVlmDfnData(
        AbsVolume<Snapshot> absVlmRef,
        DrbdVlmDfnPojo drbdVlmDfnPojoRef
    ) throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // nothing to merge
        SnapshotVolumeDefinition vlmDfn = ((SnapshotVolume) absVlmRef).getSnapshotVolumeDefinition();
        return vlmDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD, drbdVlmDfnPojoRef.getRscNameSuffix());
    }

    @Override
    protected void createOrMergeDrbdVlmData(
        AbsVolume<Snapshot> vlmRef,
        DrbdRscData<Snapshot> snapDataRef,
        DrbdVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef,
        DrbdVlmDfnData<Snapshot> drbdVlmDfnDataRef
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        DrbdVlmData<Snapshot> drbdVlmData = snapDataRef.getVlmLayerObjects().get(vlmNrRef);

        drbdVlmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        drbdVlmData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected LuksRscData<Snapshot> createLuksRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        LuksRscPojo luksRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown luks snapshot from satellite");
    }

    @Override
    protected void mergeLuksRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        LuksRscPojo luksRscPojoRef,
        LuksRscData<Snapshot> luksRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeLuksVlm(LuksRscData<Snapshot> luksRscDataRef, VolumeNumber vlmNrRef) throws DatabaseException,
        AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createOrMergeLuksVlm(
        AbsVolume<Snapshot> vlmRef,
        LuksRscData<Snapshot> luksRscDataRef,
        LuksVlmPojo vlmPojoRef
    ) throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected StorageRscData<Snapshot> createStorageRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        StorageRscPojo storRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown storage snapshot from satellite");
    }

    @Override
    protected void mergeStorageRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        StorageRscPojo storRscPojoRef,
        StorageRscData<Snapshot> storRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeStorageVlm(StorageRscData<Snapshot> storRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected VlmProviderObject<Snapshot> createDisklessVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown diskless storage snapshot volume from satellite");
    }

    @Override
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected VlmProviderObject<Snapshot> createLvmVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm storage snapshot volume from satellite");
    }

    private void mergeVlmProviderObject(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        vlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
        vlmDataRef.setExists(vlmPojoRef.exists());
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected LvmThinData<Snapshot> createLvmThinVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm thin storage snapshot volume from satellite");
    }

    @Override
    protected VlmProviderObject<Snapshot> createSpdkVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError(
            "Received unknown " + vlmPojoRef.getProviderKind() + " storage snapshot volume from satellite"
        );
    }

    @Override
    protected VlmProviderObject<Snapshot> createStorageSpacesVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown Storage Spaces storage snapshot volume from satellite");
    }

    @Override
    protected void mergeStorageSpacesVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected VlmProviderObject<Snapshot> createZfsData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown zfs storage snapshot volume from satellite");
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected VlmProviderObject<Snapshot> createFileData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Received unknown file storage snapshot volume from satellite");
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected void setStorPool(VlmProviderObject<Snapshot> vlmDataRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException
    {
        // ignored
    }

    @Override
    protected void putVlmData(StorageRscData<Snapshot> storRscDataRef, VlmProviderObject<Snapshot> vlmDataRef)
    {
        // ignored
    }

    @Override
    protected VlmProviderObject<Snapshot> createEbsData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown EBS snapshot from satellite");
    }

    @Override
    protected void mergeEbsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        mergeVlmProviderObject(vlmPojoRef, vlmDataRef);
    }

    @Override
    protected NvmeRscData<Snapshot> createNvmeRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        NvmeRscPojo nvmeRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown nvme snapshot from satellite");
    }

    @Override
    protected void mergeNvmeRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        NvmeRscPojo nvmeRscPojoRef,
        NvmeRscData<Snapshot> nvmeRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeNvmeVlm(NvmeRscData<Snapshot> nvmeRscDataRef, VolumeNumber vlmNrRef) throws DatabaseException,
        AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createNvmeVlm(
        AbsVolume<Snapshot> vlmRef,
        NvmeRscData<Snapshot> nvmeRscDataRef,
        VolumeNumber vlmNrRef
    ) throws DatabaseException
    {
        throw new ImplementationError("Missing nvme volume from satellite");
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData<Snapshot> nvmeVlmDataRef) throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected WritecacheRscData<Snapshot> createWritecacheRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        WritecacheRscPojo writecacheRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown writecache snapshot from satellite");
    }

    @Override
    protected void mergeWritecacheRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        WritecacheRscPojo writecacheRscPojoRef,
        WritecacheRscData<Snapshot> writecacheRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeWritecacheVlm(WritecacheRscData<Snapshot> writecacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createWritecacheVlm(
        AbsVolume<Snapshot> vlmRef,
        WritecacheRscData<Snapshot> writecacheRscDataRef,
        WritecacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        throw new ImplementationError("Missing writecache volume from satellite");
    }

    @Override
    protected void mergeWritecacheVlm(WritecacheVlmPojo vlmPojoRef, WritecacheVlmData<Snapshot> writecacheVlmDataRef)
        throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected CacheRscData<Snapshot> createCacheRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        CacheRscPojo cacheRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown cache snapshot from satellite");
    }

    @Override
    protected void mergeCacheRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        CacheRscPojo cacheRscPojoRef,
        CacheRscData<Snapshot> cacheRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeCacheVlm(CacheRscData<Snapshot> cacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createCacheVlm(
        AbsVolume<Snapshot> vlmRef,
        CacheRscData<Snapshot> cacheRscDataRef,
        CacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        throw new ImplementationError("Missing cache volume from satellite");
    }

    @Override
    protected void mergeCacheVlm(CacheVlmPojo vlmPojoRef, CacheVlmData<Snapshot> cacheVlmDataRef)
        throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected BCacheRscData<Snapshot> createBCacheRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        BCacheRscPojo bcacheRscPojoRef
    ) throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown bcache snapshot from satellite");
    }

    @Override
    protected void mergeBCacheRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        BCacheRscPojo bCacheRscPojoRef,
        BCacheRscData<Snapshot> bCacheRscDataRef
    ) throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeBCacheVlm(BCacheRscData<Snapshot> bcacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createBCacheVlm(
        AbsVolume<Snapshot> vlmRef,
        BCacheRscData<Snapshot> writecacheRscDataRef,
        BCacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        throw new ImplementationError("Missing bcache volume from satellite");
    }

    @Override
    protected void mergeBCacheVlm(BCacheVlmPojo vlmPojoRef, BCacheVlmData<Snapshot> bcacheVlmDataRef)
        throws DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void updateParent(AbsRscLayerObject<Snapshot> rscDataRef, AbsRscLayerObject<Snapshot> parentRef)
        throws DatabaseException
    {
        // ignored
    }

    @Override
    protected boolean wasRscLayerDataRecentlyReplaced(
        AbsRscLayerObject<Snapshot> drbdRscDataRef,
        RscLayerDataApi rscDataPojoRef
    )
    {
        return false;
    }
}
