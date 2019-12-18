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
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
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
public class StltLayerRscDataMerger extends AbsLayerRscDataMerger<Resource>
{
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final StorPoolDefinitionSatelliteFactory storPoolDefinitionFactory;
    private final StorPoolSatelliteFactory storPoolFactory;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;

    @Inject
    public StltLayerRscDataMerger(
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
    protected DrbdRscDfnData<Resource> mergeOrCreateDrbdRscDfnData(
        Resource rsc,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
        ExhaustedPoolException, ValueInUseException
    {
        ResourceDefinition rscDfn = rsc.getDefinition();
        DrbdRscDfnData<Resource> rscDfnData = rscDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdRscDfnPojo.getRscNameSuffix()
        );
        if (rscDfnData == null)
        {
            rscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn.getName(),
                null,
                drbdRscDfnPojo.getRscNameSuffix(),
                drbdRscDfnPojo.getPeerSlots(),
                drbdRscDfnPojo.getAlStripes(),
                drbdRscDfnPojo.getAlStripeSize(),
                drbdRscDfnPojo.getPort(),
                TransportType.valueOfIgnoreCase(
                    drbdRscDfnPojo.getTransportType(),
                    TransportType.IP
                ),
                drbdRscDfnPojo.getSecret()
            );
            rscDfn.setLayerData(apiCtx, rscDfnData);
        }
        else
        {
            rscDfnData.setPort(new TcpPortNumber(drbdRscDfnPojo.getPort()));
            rscDfnData.setSecret(drbdRscDfnPojo.getSecret());
            rscDfnData.setTransportType(
                TransportType.valueOfIgnoreCase(
                    drbdRscDfnPojo.getTransportType(),
                    TransportType.IP
                )
            );
        }
        return rscDfnData;
    }

    @Override
    protected DrbdRscData<Resource> createDrbdRscData(
        Resource rsc,
        RscLayerDataApi rscDataPojo,
        AbsRscLayerObject<Resource> parent,
        DrbdRscPojo drbdRscPojo,
        DrbdRscDfnData<Resource> drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        DrbdRscData<Resource> drbdRscData;
        drbdRscData = layerDataFactory.createDrbdRscData(
            rscDataPojo.getId(),
            rsc,
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
            rsc.setLayerData(apiCtx, drbdRscData);
        }
        else
        {
            parent.getChildren().add(drbdRscData);
        }
        return drbdRscData;
    }

    @Override
    protected void mergeDrbdRscData(
        AbsRscLayerObject<Resource> parentRef,
        DrbdRscPojo drbdRscPojoRef,
        DrbdRscData<Resource> drbdRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(drbdRscPojoRef.getFlags())
        );
        updateParent(drbdRscDataRef, parentRef);
        drbdRscDataRef.setSuspendIo(drbdRscPojoRef.getSuspend());
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData<Resource> drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected DrbdVlmDfnData<Resource> mergeOrCreateDrbdVlmDfnData(
        AbsVolume<Resource> vlm,
        DrbdVlmDfnPojo drbdVlmDfnPojoRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        DrbdVlmDfnData<Resource> drbdVlmDfnData = vlmDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdVlmDfnPojoRef.getRscNameSuffix()
        );
        if (drbdVlmDfnData == null)
        {
            drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                vlmDfn,
                vlmDfn.getResourceDefinition().getName(),
                null,
                drbdVlmDfnPojoRef.getRscNameSuffix(),
                vlmDfn.getVolumeNumber(),
                drbdVlmDfnPojoRef.getMinorNr(),
                vlmDfn.getResourceDefinition().getLayerData(
                    apiCtx,
                    DeviceLayerKind.DRBD,
                    drbdVlmDfnPojoRef.getRscNameSuffix()
                )
            );
            vlmDfn.setLayerData(apiCtx, drbdVlmDfnData);
        }
        else
        {
            // nothing to merge
        }
        return drbdVlmDfnData;
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
    protected LuksRscData<Resource> createLuksRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        LuksRscPojo luksRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        LuksRscData<Resource> luksRscData = layerDataFactory.createLuksRscData(
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
    protected void removeLuksVlm(LuksRscData<Resource> luksRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        luksRscDataRef.remove(apiCtx, vlmNrRef);
    }


    @Override
    protected void createOrMergeLuksVlm(
        AbsVolume<Resource> vlmRef,
        LuksRscData<Resource> luksRscDataRef,
        LuksVlmPojo vlmPojoRef
    )
        throws DatabaseException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        LuksVlmData<Resource> luksVlmData = luksRscDataRef.getVlmLayerObjects().get(vlmNr);
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
    protected StorageRscData<Resource> createStorageRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        StorageRscPojo storRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        StorageRscData<Resource> storRscData = layerDataFactory.createStorageRscData(
            storRscPojoRef.getId(),
            parentRef,
            rscRef,
            storRscPojoRef.getRscNameSuffix()
        );
        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, storRscData);
        }
        else
        {
            updateParent(storRscData, parentRef);
        }
        return storRscData;
    }

    @Override
    protected void removeStorageVlm(StorageRscData<Resource> storRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        storRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected StorPool getStoragePool(
        AbsVolume<Resource> vlm,
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
    protected VlmProviderObject<Resource> createDisklessVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
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
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Resource> createLvmVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createLvmData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected LvmThinData<Resource> createLvmThinVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createLvmThinData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected VlmProviderObject<Resource> createSpdkVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        StorPool storPoolRef
    )
            throws DatabaseException
    {
        return layerDataFactory.createSpdkData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject<Resource> vlmData)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Resource> createZfsData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createZfsData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject<Resource> createFileData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createFileData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected void setStorPool(VlmProviderObject<Resource> vlmDataRef, StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException
    {
        vlmDataRef.setStorPool(apiCtx, storPoolRef);
    }

    @Override
    protected void putVlmData(
        StorageRscData<Resource> storRscDataRef,
        VlmProviderObject<Resource> vlmDataRef
    )
    {
        storRscDataRef.getVlmLayerObjects().put(vlmDataRef.getVlmNr(), vlmDataRef);
    }

    @Override
    protected NvmeRscData<Resource> createNvmeRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        NvmeRscPojo nvmeRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        NvmeRscData<Resource> nvmeRscData = layerDataFactory.createNvmeRscData(
            nvmeRscPojoRef.getId(),
            rscRef,
            nvmeRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, nvmeRscData);
        }
        else
        {
            updateParent(nvmeRscData, parentRef);
        }
        return nvmeRscData;
    }

    @Override
    protected void removeNvmeVlm(NvmeRscData<Resource> nvmeRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        nvmeRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createNvmeVlm(
        AbsVolume<Resource> vlmRef,
        NvmeRscData<Resource> nvmeRscDataRef,
        VolumeNumber vlmNrRef
    )
    {
        NvmeVlmData<Resource> nvmeVlmData = layerDataFactory.createNvmeVlmData(vlmRef, nvmeRscDataRef);
        nvmeRscDataRef.getVlmLayerObjects().put(vlmNrRef, nvmeVlmData);
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData<Resource> nvmeVlmDataRef)
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring diskState
        // ignoring usableSize
    }

    @Override
    protected WritecacheRscData<Resource> createWritecacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        WritecacheRscPojo writecacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        WritecacheRscData<Resource> writecacheRscData;
        writecacheRscData = layerDataFactory.createWritecacheRscData(
            writecacheRscPojoRef.getId(),
            rscRef,
            writecacheRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, writecacheRscData);
        }
        else
        {
            updateParent(writecacheRscData, parentRef);
        }
        return writecacheRscData;
    }

    @Override
    protected void removeWritecacheVlm(WritecacheRscData<Resource> writecacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        writecacheRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createWritecacheVlm(
        AbsVolume<Resource> vlmRef,
        WritecacheRscData<Resource> writecacheRscDataRef,
        WritecacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    ) throws AccessDeniedException, InvalidNameException
    {
        String cacheStorPoolNameStr = vlmPojo.getCacheStorPoolName();
        StorPool cacheStorPool = null;
        if (cacheStorPoolNameStr != null && !cacheStorPoolNameStr.trim().isEmpty())
        {
            cacheStorPool = vlmRef.getAbsResource().getNode().getStorPool(
                apiCtx,
                new StorPoolName(cacheStorPoolNameStr)
            );
        }

        WritecacheVlmData<Resource> writecacheVlmData = layerDataFactory.createWritecacheVlmData(
            vlmRef,
            cacheStorPool,
            writecacheRscDataRef
        );
        writecacheRscDataRef.getVlmLayerObjects().put(vlmNrRef, writecacheVlmData);
    }

    @Override
    protected void mergeWritecacheVlm(
        WritecacheVlmPojo vlmPojo,
        WritecacheVlmData<Resource> writecacheVlmData
    )
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring devicePathCache
        // ignoring diskState
        // ignoring exists
        // ignoring identifier
        // ignoring usableSize
        // ignoring cacheStorPool (cannot be updated / changed)
    }

    @Override
    protected void updateParent(
        AbsRscLayerObject<Resource> child,
        AbsRscLayerObject<Resource> newParent
    )
        throws DatabaseException
    {
        AbsRscLayerObject<Resource> oldParent = child.getParent();
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
}
