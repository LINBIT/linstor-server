package com.linbit.linstor.core.apicallhandler;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
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
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.NodeId;
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
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StltLayerSnapDataMerger extends AbsLayerRscDataMerger<Snapshot>
{
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final StorPoolDefinitionSatelliteFactory storPoolDefinitionFactory;
    private final StorPoolSatelliteFactory storPoolFactory;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;

    @Inject
    public StltLayerSnapDataMerger(
        @SystemContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        StorPoolDefinitionSatelliteFactory storPoolDefinitionFactoryRef,
        StorPoolSatelliteFactory storPoolFactoryRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef
    )
    {
        super(apiCtxRef, layerDataFactoryRef);
        storPoolDfnMap = storPoolDfnMapRef;
        storPoolDefinitionFactory = storPoolDefinitionFactoryRef;
        storPoolFactory = storPoolFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
    }

    @Override
    protected DrbdRscDfnData<Snapshot> mergeOrCreateDrbdRscDfnData(
        Snapshot snap,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
        ExhaustedPoolException, ValueInUseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        DrbdRscDfnData<Snapshot> snapDfnData = snapDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdRscDfnPojo.getRscNameSuffix()
        );
        if (snapDfnData == null)
        {
            snapDfnData = layerDataFactory.createDrbdRscDfnData(
                snapDfn.getResourceName(),
                snapDfn.getName(),
                drbdRscDfnPojo.getRscNameSuffix(),
                drbdRscDfnPojo.getPeerSlots(),
                drbdRscDfnPojo.getAlStripes(),
                drbdRscDfnPojo.getAlStripeSize(),
                DrbdRscDfnData.SNAPSHOT_TCP_PORT,
                TransportType.valueOfIgnoreCase(
                    drbdRscDfnPojo.getTransportType(),
                    TransportType.IP
                ),
                null
            );
            snapDfn.setLayerData(apiCtx, snapDfnData);
        }
        else
        {
            snapDfnData.setTransportType(
                TransportType.valueOfIgnoreCase(
                    drbdRscDfnPojo.getTransportType(),
                    TransportType.IP
                )
            );
        }
        return snapDfnData;
    }

    @Override
    protected DrbdRscData<Snapshot> createDrbdRscData(
        Snapshot snap,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<Snapshot> parent,
        DrbdRscPojo drbdRscPojo,
        DrbdRscDfnData<Snapshot> drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        DrbdRscData<Snapshot> drbdRscData = layerDataFactory.createDrbdRscData(
            rscDataPojo.getId(),
            snap,
            rscDataPojo.getRscNameSuffix(),
            parent,
            drbdRscDfnData,
            new NodeId(drbdRscPojo.getNodeId()),
            drbdRscPojo.getPeerSlots(),
            drbdRscPojo.getAlStripes(),
            drbdRscPojo.getAlStripeSize(),
            drbdRscPojo.getFlags()
        );
        drbdRscDfnData.getDrbdRscDataList().add(drbdRscData);
        if (parent == null)
        {
            snap.setLayerData(apiCtx, drbdRscData);
        }
        else
        {
            parent.getChildren().add(drbdRscData);
        }
        return drbdRscData;
    }

    @Override
    protected void mergeDrbdRscData(
        AbsRscLayerObject<Snapshot> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscData<Snapshot> drbdRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(drbdRscPojoRef.getFlags())
        );
        updateParent(drbdRscDataRef, parentRef);
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData<Snapshot> drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected DrbdVlmDfnData<Snapshot> mergeOrCreateDrbdVlmDfnData(
        AbsVolume<Snapshot> absVlm,
        DrbdVlmDfnPojo drbdVlmDfnPojoRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        SnapshotVolumeDefinition snapVlmDfn = ((SnapshotVolume) absVlm).getSnapshotVolumeDefinition();
        DrbdVlmDfnData<Snapshot> drbdVlmDfnData = snapVlmDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdVlmDfnPojoRef.getRscNameSuffix()
        );
        if (drbdVlmDfnData == null)
        {
            drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                snapVlmDfn.getVolumeDefinition(),
                snapVlmDfn.getResourceName(),
                snapVlmDfn.getSnapshotName(),
                drbdVlmDfnPojoRef.getRscNameSuffix(),
                snapVlmDfn.getVolumeNumber(),
                DrbdVlmDfnData.SNAPSHOT_MINOR,
                snapVlmDfn.getSnapshotDefinition().getLayerData(
                    apiCtx,
                    DeviceLayerKind.DRBD,
                    drbdVlmDfnPojoRef.getRscNameSuffix()
                )
            );
            snapVlmDfn.setLayerData(apiCtx, drbdVlmDfnData);
        }
        else
        {
            // nothing to merge
        }
        return drbdVlmDfnData;
    }

    @Override
    protected void createOrMergeDrbdVlmData(
        AbsVolume<Snapshot> vlmRef,
        DrbdRscData<Snapshot> rscDataRef,
        DrbdVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef,
        DrbdVlmDfnData<Snapshot> drbdVlmDfnDataRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        DrbdVlmData<Snapshot> drbdVlmData = rscDataRef.getVlmLayerObjects().get(vlmNrRef);

        StorPool extMetaStorPool = null;
        String extMetaStorPoolNameStr = vlmPojoRef.getExternalMetaDataStorPool();
        if (extMetaStorPoolNameStr != null)
        {
            extMetaStorPool = vlmRef.getAbsResource().getNode().getStorPool(
                apiCtx,
                new StorPoolName(extMetaStorPoolNameStr)
            );
        }
        if (drbdVlmData == null)
        {
            drbdVlmData = layerDataFactory.createDrbdVlmData(
                vlmRef,
                extMetaStorPool,
                rscDataRef,
                drbdVlmDfnDataRef
            );
            rscDataRef.getVlmLayerObjects().put(vlmNrRef, drbdVlmData);
        }
        else
        {
            // ignore allocatedSize
            // ignore devicePath
            // ignore diskState
            // ignore usableSize
            drbdVlmData.setExternalMetaDataStorPool(extMetaStorPool);
        }
    }

    @Override
    protected LuksRscData<Snapshot> createLuksRscData(
        Snapshot rscRef,
        AbsRscLayerObject<Snapshot> parentRef,
        LuksRscPojo luksRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        LuksRscData<Snapshot> luksRscData = layerDataFactory.createLuksRscData(
            luksRscPojoRef.getId(),
            rscRef,
            luksRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, luksRscData);
        }
        else
        {
            updateParent(luksRscData, parentRef);
        }
        return luksRscData;
    }

    @Override
    protected void removeLuksVlm(LuksRscData<Snapshot> luksRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        luksRscDataRef.remove(apiCtx, vlmNrRef);
    }


    @Override
    protected void createOrMergeLuksVlm(
        AbsVolume<Snapshot> vlmRef,
        LuksRscData<Snapshot> luksRscDataRef,
        LuksVlmPojo vlmPojoRef
    )
        throws DatabaseException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        LuksVlmData<Snapshot> luksVlmData = luksRscDataRef.getVlmLayerObjects().get(vlmNr);
        if (luksVlmData == null)
        {
            luksVlmData = layerDataFactory.createLuksVlmData(
                vlmRef,
                luksRscDataRef,
                vlmPojoRef.getEncryptedPassword()
            );
            luksRscDataRef.getVlmLayerObjects().put(vlmNr, luksVlmData);
        }
        else
        {
            // ignoring allocatedSize
            // ignoring backingDevice
            // ignoring devicePath
            // ignoring opened
            // ignoring diskState
            // ignoring usableSize
        }
    }

    @Override
    protected StorageRscData<Snapshot> createStorageRscData(
        Snapshot snapRef,
        AbsRscLayerObject<Snapshot> parentRef,
        StorageRscPojo storRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        StorageRscData<Snapshot> storSnapData = layerDataFactory.createStorageRscData(
            storRscPojoRef.getId(),
            parentRef,
            snapRef,
            storRscPojoRef.getRscNameSuffix()
        );
        if (parentRef == null)
        {
            snapRef.setLayerData(apiCtx, storSnapData);
        }
        else
        {
            updateParent(storSnapData, parentRef);
        }
        return storSnapData;
    }

    @Override
    protected void removeStorageVlm(StorageRscData<Snapshot> storSnapDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        storSnapDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected StorPool getStoragePool(
        AbsVolume<Snapshot> vlm,
        VlmLayerDataApi vlmPojo,
        boolean remoteResource
    )
        throws InvalidNameException, AccessDeniedException
    {
        StorPool storPool = super.getStoragePool(vlm, vlmPojo, remoteResource);
        if (storPool == null)
        {
            if (remoteResource)
            {
                StorPoolApi storPoolApi = vlmPojo.getStorPoolApi();

                StorPoolDefinition storPoolDfn = storPoolDfnMap.get(new StorPoolName(storPoolApi.getStorPoolName()));
                if (storPoolDfn == null)
                {
                    storPoolDfn = storPoolDefinitionFactory.getInstance(
                        apiCtx,
                        storPoolApi.getStorPoolDfnUuid(),
                        new StorPoolName(storPoolApi.getStorPoolName())
                    );
                    storPoolDfn.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolDfnProps());
                    storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
                }
                storPool = storPoolFactory.getInstanceSatellite(
                    apiCtx,
                    storPoolApi.getStorPoolUuid(),
                    vlm.getAbsResource().getNode(),
                    storPoolDfn,
                    storPoolApi.getDeviceProviderKind(),
                    freeSpaceMgrFactory.getInstance()
                );
                storPool.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolProps());
            }
            else
            {
                throw new ImplementationError("Unknown storage pool '" + vlmPojo.getStorPoolApi().getStorPoolName() +
                    "' for volume " + vlm);
            }
        }
        return storPool;
    }

    @Override
    protected VlmProviderObject<Snapshot> createDisklessVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createDisklessData(
            vlmRef,
            vlmPojoRef.getUsableSize(),
            storRscDataRef,
            storPoolRef
        );
    }

    @Override
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Snapshot> createLvmVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createLvmData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected LvmThinData<Snapshot> createLvmThinVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createLvmThinData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Snapshot> createZfsData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storSnapDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createZfsData(vlmRef, storSnapDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Snapshot> createFileData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storSnapDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createFileData(vlmRef, storSnapDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected void setStorPool(VlmProviderObject<Snapshot> vlmDataRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException
    {
        vlmDataRef.setStorPool(apiCtx, storPoolRef);
    }

    @Override
    protected void putVlmData(
        StorageRscData<Snapshot> storSnapDataRef,
        VlmProviderObject<Snapshot> vlmDataRef
    )
    {
        storSnapDataRef.getVlmLayerObjects().put(vlmDataRef.getVlmNr(), vlmDataRef);
    }

    @Override
    protected NvmeRscData<Snapshot> createNvmeRscData(
        Snapshot snapRef,
        AbsRscLayerObject<Snapshot> parentRef,
        NvmeRscPojo nvmeRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        NvmeRscData<Snapshot> nvmeRscData = layerDataFactory.createNvmeRscData(
            nvmeRscPojoRef.getId(),
            snapRef,
            nvmeRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        if (parentRef == null)
        {
            snapRef.setLayerData(apiCtx, nvmeRscData);
        }
        else
        {
            updateParent(nvmeRscData, parentRef);
        }
        return nvmeRscData;
    }

    @Override
    protected void removeNvmeVlm(NvmeRscData<Snapshot> nvmeSnapDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        nvmeSnapDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createNvmeVlm(
        AbsVolume<Snapshot> vlmRef,
        NvmeRscData<Snapshot> nvmeSnapDataRef,
        VolumeNumber vlmNrRef
    )
    {
        NvmeVlmData<Snapshot> nvmeVlmData = layerDataFactory.createNvmeVlmData(vlmRef, nvmeSnapDataRef);
        nvmeSnapDataRef.getVlmLayerObjects().put(vlmNrRef, nvmeVlmData);
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData<Snapshot> nvmeVlmDataRef)
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring diskState
        // ignoring usableSize
    }

    @Override
    protected void updateParent(
        AbsRscLayerObject<Snapshot> child,
        AbsRscLayerObject<Snapshot> newParent
    )
        throws DatabaseException
    {
        AbsRscLayerObject<Snapshot> oldParent = child.getParent();
        if (oldParent != null)
        {
            oldParent.getChildren().remove(child);
        }

        child.setParent(newParent);

        if (newParent != null)
        {
            newParent.getChildren().add(child);
        }
    }

    @Override
    protected WritecacheRscData<Snapshot> createWritecacheRscData(
        Snapshot snapRef,
        AbsRscLayerObject<Snapshot> parentRef,
        WritecacheRscPojo writecacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        WritecacheRscData<Snapshot> writecacheSnapData = layerDataFactory.createWritecacheRscData(
            writecacheRscPojoRef.getId(),
            snapRef,
            writecacheRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        if (parentRef == null)
        {
            snapRef.setLayerData(apiCtx, writecacheSnapData);
        }
        else
        {
            updateParent(writecacheSnapData, parentRef);
        }
        return writecacheSnapData;
    }

    @Override
    protected void removeWritecacheVlm(
        WritecacheRscData<Snapshot> writecacheRscDataRef,
        VolumeNumber vlmNrRef
    )
        throws DatabaseException, AccessDeniedException
    {
        writecacheRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createWritecacheVlm(
        AbsVolume<Snapshot> vlmRef,
        WritecacheRscData<Snapshot> writecacheRscDataRef,
        WritecacheVlmPojo vlmPojoRef,
        VolumeNumber vlmNrRef
    ) throws AccessDeniedException, InvalidNameException
    {
        String cacheStorPoolNameStr = vlmPojoRef.getCacheStorPoolName();
        StorPool cacheStorPool = null;
        if (cacheStorPoolNameStr != null && !cacheStorPoolNameStr.trim().isEmpty())
        {
            cacheStorPool = vlmRef.getAbsResource().getNode().getStorPool(
                apiCtx,
                new StorPoolName(cacheStorPoolNameStr)
            );
        }

        WritecacheVlmData<Snapshot> writecacheVlmData = layerDataFactory.createWritecacheVlmData(
            vlmRef,
            cacheStorPool,
            writecacheRscDataRef
        );

        writecacheRscDataRef.getVlmLayerObjects().put(vlmNrRef, writecacheVlmData);
    }

    @Override
    protected void mergeWritecacheVlm(WritecacheVlmPojo vlmPojoRef, WritecacheVlmData<Snapshot> writecacheVlmDataRef)
        throws DatabaseException
    {
        // (for now) ignoring everythin
    }

    @Override
    protected VlmProviderObject<Snapshot> createSpdkVlmData(
        AbsVolume<Snapshot> vlmRef,
        StorageRscData<Snapshot> storRscDataRef,
        StorPool storPoolRef
    ) throws DatabaseException
    {
        return layerDataFactory.createSpdkData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Snapshot> vlmDataRef)
        throws DatabaseException
    {
        // nothing special to merge
    }
}
