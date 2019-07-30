package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.VolumeNumber;
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
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlRscLayerDataMerger extends AbsLayerRscDataMerger
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
    protected DrbdRscDfnData mergeOrCreateDrbdRscDfnData(
        ResourceDefinition rscDfn,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
            ExhaustedPoolException, ValueInUseException
    {
        // nothing to merge
        return rscDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdRscDfnPojo.getRscNameSuffix()
        );
    }

    @Override
    protected DrbdRscData createDrbdRscData(
        Resource rscRef, RscLayerDataApi rscDataPojoRef, RscLayerObject parentRef, DrbdRscPojo drbdRscPojoRef,
        DrbdRscDfnData drbdRscDfnDataRef
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown drbd resource from satellite");
    }

    @Override
    protected void mergeDrbdRscData(RscLayerObject parentRef, DrbdRscPojo drbdRscPojoRef, DrbdRscData drbdRscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // nothing to merge
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected DrbdVlmDfnData mergeOrCreateDrbdVlmDfnData(VolumeDefinition vlmDfnRef, DrbdVlmDfnPojo drbdVlmDfnPojoRef)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // nothing to merge
        return vlmDfnRef.getLayerData(apiCtx, DeviceLayerKind.DRBD, drbdVlmDfnPojoRef.getRscNameSuffix());
    }

    @Override
    protected void createOrMergeDrbdVlmData(
        Volume vlmRef,
        DrbdRscData rscDataRef,
        DrbdVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef,
        DrbdVlmDfnData drbdVlmDfnDataRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        DrbdVlmData drbdVlmData = rscDataRef.getVlmLayerObjects().get(vlmNrRef);

        drbdVlmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        drbdVlmData.setDevicePath(vlmPojoRef.getDevicePath());
        drbdVlmData.setDiskState(vlmPojoRef.getDiskState());
        drbdVlmData.setUsableSize(vlmPojoRef.getUsableSize());
        // ignore externalMetaDataStorPool
    }

    @Override
    protected LuksRscData createLuksRscData(Resource rscRef, RscLayerObject parentRef, LuksRscPojo luksRscPojoRef)
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown luks resource from satellite");
    }

    @Override
    protected void removeLuksVlm(LuksRscData luksRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void createOrMergeLuksVlm(Volume vlmRef, LuksRscData luksRscDataRef, LuksVlmPojo vlmPojoRef)
        throws DatabaseException
    {
        LuksVlmData luksVlmData = luksRscDataRef.getVlmLayerObjects().get(
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
    protected StorageRscData createStorageRscData(
        Resource rscRef, RscLayerObject parentRef, StorageRscPojo storRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown storage resource from satellite");
    }

    @Override
    protected void removeStorageVlm(StorageRscData storRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void updateParent(RscLayerObject rscDataRef, RscLayerObject parentRef) throws DatabaseException
    {
        // ignored
    }

    @Override
    protected VlmProviderObject createDisklessVlmData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown diskless storage volume from satellite");
    }

    @Override
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        ((DisklessData) vlmDataRef).setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected VlmProviderObject createLvmVlmData(Volume vlmRef, StorageRscData storRscDataRef, StorPool storPoolRef)
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm storage volume from satellite");
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        LvmData lvmData = (LvmData) vlmDataRef;
        lvmData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        lvmData.setDevicePath(vlmPojoRef.getDevicePath());
        lvmData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected LvmThinData createLvmThinVlmData(Volume vlmRef, StorageRscData storRscDataRef, StorPool storPoolRef)
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown lvm thin storage volume from satellite");
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        LvmThinData lvmThinData = (LvmThinData) vlmDataRef;
        lvmThinData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        lvmThinData.setDevicePath(vlmPojoRef.getDevicePath());
        lvmThinData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected VlmProviderObject createSfInitVlmData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown swordfish initiator storage volume from satellite");
    }

    @Override
    protected void mergeSfInitVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        SfInitiatorData sfInitData = (SfInitiatorData) vlmDataRef;
        sfInitData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        sfInitData.setDevicePath(vlmPojoRef.getDevicePath());
        sfInitData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected VlmProviderObject createSfTargetVlmData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown swordfish target storage volume from satellite");
    }

    @Override
    protected void mergeSfTargetVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        SfTargetData sfTargetData = (SfTargetData) vlmDataRef;
        sfTargetData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
    }

    @Override
    protected VlmProviderObject createZfsData(
        Volume vlm,
        StorageRscData storRscData,
        VlmLayerDataApi vlmDataApi,
        StorPool StorPool
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown zfs storage volume from satellite");
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef)
        throws DatabaseException
    {
        ZfsData zfsData = (ZfsData) vlmDataRef;
        zfsData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        zfsData.setDevicePath(vlmPojoRef.getDevicePath());
        zfsData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected VlmProviderObject createFileData(
        Volume vlm,
        StorageRscData storRsc,
        VlmLayerDataApi vlmDataApi,
        StorPool storPool
    )
        throws DatabaseException
    {
        throw new ImplementationError("Received unknown file storage volume from satellite");
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef)
        throws DatabaseException
    {
        FileData fileData = (FileData) vlmDataRef;
        fileData.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        fileData.setDevicePath(vlmPojoRef.getDevicePath());
        fileData.setUsableSize(vlmPojoRef.getUsableSize());
    }

    @Override
    protected void setStorPool(VlmProviderObject vlmDataRef, StorPool storPoolRef)
    {
        // ignored
    }

    @Override
    protected void putVlmData(StorageRscData storRscDataRef, VlmProviderObject vlmDataRef)
    {
        // ignored
    }

    @Override
    protected NvmeRscData createNvmeRscData(Resource rscRef, RscLayerObject parentRef, NvmeRscPojo nvmeRscPojoRef)
        throws DatabaseException, AccessDeniedException
    {
        throw new ImplementationError("Received unknown nvme resource from satellite");
    }

    @Override
    protected void createNvmeVlm(Volume vlmRef, NvmeRscData nvmeRscDataRef, VolumeNumber vlmNrRef)
    {
        throw new ImplementationError("Missing luks volume from satellite");
    }

    @Override
    protected void removeNvmeVlm(NvmeRscData nvmeRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        // ignored. A parent volume might have more volumes in one of its children than in an other one
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData nvmeVlmDataRef)
    {
        nvmeVlmDataRef.setAllocatedSize(vlmPojoRef.getAllocatedSize());
        nvmeVlmDataRef.setDevicePath(vlmPojoRef.getDevicePath());
        nvmeVlmDataRef.setDiskState(vlmPojoRef.getDiskState());
        nvmeVlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
    }

}
