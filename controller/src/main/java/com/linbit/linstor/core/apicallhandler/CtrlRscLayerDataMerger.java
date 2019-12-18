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
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlRscLayerDataMerger extends AbsLayerRscDataMerger<Resource>
{
    @Inject
    public CtrlRscLayerDataMerger(
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef
    )
    {
        super(apiCtxRef, layerDataFactoryRef);
    }

    @Override
    public void mergeLayerData(Resource rscRef, RscLayerDataApi rscLayerDataPojoRef, boolean remoteResourceRef)
    {
        try
        {
            super.mergeLayerData(rscRef, rscLayerDataPojoRef, remoteResourceRef);
        }
        catch (NullPointerException npe)
        {
            throw new ImplementationError("Received unknown object from satellite", npe);
        }
    }

    @Override
    protected DrbdRscDfnData<Resource> mergeOrCreateDrbdRscDfnData(
        Resource rsc,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
            ExhaustedPoolException, ValueInUseException
    {
        // nothing to merge
        return rsc.getDefinition().getLayerData(
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
        luksVlmData.setBackingDevice(vlmPojoRef.getBackingDevice());
        luksVlmData.setDevicePath(vlmPojoRef.getDevicePath());
        luksVlmData.setOpened(vlmPojoRef.isOpened());
        luksVlmData.setDiskState(vlmPojoRef.getDiskState());
        luksVlmData.setUsableSize(vlmPojoRef.getUsableSize());
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
        ((DisklessData<Resource>) vlmDataRef).setUsableSize(vlmPojoRef.getUsableSize());
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
    }

    @Override
    protected VlmProviderObject<Resource> createSpdkVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
            throws DatabaseException
    {
        throw new ImplementationError("Received unknown spdk storage volume from satellite");
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        SpdkData<Resource> spdkData = (SpdkData<Resource>) vlmDataRef;
        spdkData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        spdkData.setDevicePath(vlmPojoRef.getDevicePath());
        spdkData.setUsableSize(vlmPojoRef.getUsableSize());
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
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData<Resource> nvmeVlmDataRef)
    {
        nvmeVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        nvmeVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        nvmeVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        nvmeVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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
        writecacheVlmDataRef.setCacheDevice(vlmPojoRef.getDevicePathCache());
        writecacheVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        writecacheVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
    }

}
