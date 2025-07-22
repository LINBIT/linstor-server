package com.linbit.linstor.core.objects.merger;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
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
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.AbsLayerRscDataMerger;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
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
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
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
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Objects;
import java.util.Set;

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
        throws IllegalArgumentException, DatabaseException, AccessDeniedException, ValueOutOfRangeException
    {
        ResourceDefinition rscDfn = rsc.getResourceDefinition();
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
                // stlt does not care about the preferred DrbdRscDfnData's port. Stlt will only use DrbdRscData's port
                null,
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
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException, ExhaustedPoolException,
        ValueInUseException
    {
        DrbdRscData<Resource> drbdRscData;
        @Nullable Set<Integer> ports = drbdRscPojo.getPorts();
        drbdRscData = layerDataFactory.createDrbdRscData(
            rscDataPojo.getId(),
            rsc,
            rscDataPojo.getRscNameSuffix(),
            parent,
            drbdRscDfnData,
            new NodeId(drbdRscPojo.getNodeId()),
            ports == null ? null : TcpPortNumber.parse(ports),
            drbdRscPojo.getPortCount(),
            drbdRscPojo.getPeerSlots(),
            drbdRscPojo.getAlStripes(),
            drbdRscPojo.getAlStripeSize(),
            drbdRscPojo.getFlags()
        );
        drbdRscData.addAllIgnoreReasons(drbdRscPojo.getIgnoreReasons());

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
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, ImplementationError
    {
        drbdRscDataRef.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(drbdRscPojoRef.getFlags())
        );
        if (drbdRscPojoRef.getNodeId() != drbdRscDataRef.getNodeId().value)
        {
            try
            {
                drbdRscDataRef.setNodeId(new NodeId(drbdRscPojoRef.getNodeId()));
            }
            catch (DatabaseException | ValueOutOfRangeException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        if (drbdRscPojoRef.getPortCount() != null && drbdRscPojoRef.getPortCount() != drbdRscDataRef.getPortCount())
        {
            drbdRscDataRef.setPortCount(drbdRscPojoRef.getPortCount());
        }
        @Nullable Set<Integer> ports = drbdRscPojoRef.getPorts();
        if (ports != null)
        {
            drbdRscDataRef.setPorts(TcpPortNumber.parse(ports));
        }
        updateParent(drbdRscDataRef, parentRef);
        drbdRscDataRef.setShouldSuspendIo(drbdRscPojoRef.getSuspend());
        drbdRscDataRef.resetIgnoreReasonsTo(drbdRscPojoRef.getIgnoreReasons());
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
        luksRscData.addAllIgnoreReasons(luksRscPojoRef.getIgnoreReasons());

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
    protected void mergeLuksRscData(
        AbsRscLayerObject<Resource> parentRef,
        LuksRscPojo luksRscPojoRef,
        LuksRscData<Resource> luksRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        luksRscDataRef.setShouldSuspendIo(luksRscPojoRef.getSuspend());
        luksRscDataRef.resetIgnoreReasonsTo(luksRscPojoRef.getIgnoreReasons());
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
            // with snapshot-shipping, the password might actually change!
            // in that case the resource should also have the REACTIVATE flag, which triggers
            // a new decryption of the new key
            luksVlmData.setEncryptedKey(vlmPojoRef.getEncryptedPassword());
            luksVlmData.setModifyPassword(vlmPojoRef.getModifyPassword());

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
        storRscData.addAllIgnoreReasons(storRscPojoRef.getIgnoreReasons());
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
    protected void mergeStorageRscData(
        AbsRscLayerObject<Resource> parentRef,
        StorageRscPojo storRscPojoRef,
        StorageRscData<Resource> storRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        storRscDataRef.setShouldSuspendIo(storRscPojoRef.getSuspend());

        final boolean hadIgnoreReason = storRscDataRef.hasAnyPreventExecutionIgnoreReason();
        storRscDataRef.resetIgnoreReasonsTo(storRscPojoRef.getIgnoreReasons());
        boolean hasLvm = false;
        for (VlmProviderObject<Resource> storVlmData : storRscDataRef.getVlmLayerObjects().values())
        {
            DeviceProviderKind providerKind = storVlmData.getProviderKind();
            if (providerKind.equals(DeviceProviderKind.LVM) || providerKind.equals(DeviceProviderKind.LVM_THIN))
            {
                hasLvm = true;
                break;
            }
        }
        if (hasLvm && hadIgnoreReason && !storRscDataRef.hasAnyPreventExecutionIgnoreReason())
        {
            LvmUtils.recacheNext();
        }
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
                DeviceProviderKind deviceProviderKind = storPoolApi.getDeviceProviderKind();
                storPool = storPoolFactory.getInstanceSatellite(
                    apiCtx,
                    storPoolApi.getStorPoolUuid(),
                    vlm.getAbsResource().getNode(),
                    storPoolDfn,
                    deviceProviderKind,
                    freeSpaceMgrFactory.getInstance(
                        SharedStorPoolName.restoreName(storPoolApi.getFreeSpaceManagerName())
                    ),
                    storPoolApi.isExternalLocking()
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

        // The local usable size is going to be recalculated (and will override the pojo's value),
        // whereas the remote usable sizes will be used AbsStorageProvider#getSmallestCommonUsableStorageSize
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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
    protected VlmProviderObject<Resource> createStorageSpacesVlmData(
        AbsVolume<Resource> vlmRef,
        StorageRscData<Resource> storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createStorageSpacesData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeStorageSpacesVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath

        // The local usable size is going to be recalculated (and will override the pojo's value),
        // whereas the remote usable sizes will be used AbsStorageProvider#getSmallestCommonUsableStorageSize
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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
        return layerDataFactory.createSpdkData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
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

        // The local usable size is going to be recalculated (and will override the pojo's value),
        // whereas the remote usable sizes will be used AbsStorageProvider#getSmallestCommonUsableStorageSize
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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

        // The local usable size is going to be recalculated (and will override the pojo's value),
        // whereas the remote usable sizes will be used AbsStorageProvider#getSmallestCommonUsableStorageSize
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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

        // The local usable size is going to be recalculated (and will override the pojo's value),
        // whereas the remote usable sizes will be used AbsStorageProvider#getSmallestCommonUsableStorageSize
        vlmDataRef.setUsableSize(vlmPojoRef.getUsableSize());
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
        nvmeRscData.addAllIgnoreReasons(nvmeRscPojoRef.getIgnoreReasons());

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
    protected void mergeNvmeRscData(
        AbsRscLayerObject<Resource> parentRef,
        NvmeRscPojo nvmeRscPojoRef,
        NvmeRscData<Resource> nvmeRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        nvmeRscDataRef.setShouldSuspendIo(nvmeRscPojoRef.getSuspend());
        nvmeRscDataRef.resetIgnoreReasonsTo(nvmeRscPojoRef.getIgnoreReasons());
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
        throws DatabaseException
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
        writecacheRscData.addAllIgnoreReasons(writecacheRscPojoRef.getIgnoreReasons());

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
    protected void mergeWritecacheRscData(
        AbsRscLayerObject<Resource> parent,
        WritecacheRscPojo writecacheRscPojo,
        WritecacheRscData<Resource> writecacheRscData)
        throws AccessDeniedException, DatabaseException
    {
        writecacheRscData.setShouldSuspendIo(writecacheRscPojo.getSuspend());
        writecacheRscData.resetIgnoreReasonsTo(writecacheRscPojo.getIgnoreReasons());
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
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
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
    protected CacheRscData<Resource> createCacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        CacheRscPojo cacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        CacheRscData<Resource> cacheRscData;
        cacheRscData = layerDataFactory.createCacheRscData(
            cacheRscPojoRef.getId(),
            rscRef,
            cacheRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        cacheRscData.addAllIgnoreReasons(cacheRscPojoRef.getIgnoreReasons());

        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, cacheRscData);
        }
        else
        {
            updateParent(cacheRscData, parentRef);
        }
        return cacheRscData;
    }

    @Override
    protected void mergeCacheRscData(
        AbsRscLayerObject<Resource> parentRef,
        CacheRscPojo cacheRscPojoRef,
        CacheRscData<Resource> cacheRscDataRef
    )
        throws AccessDeniedException, DatabaseException
    {
        cacheRscDataRef.setShouldSuspendIo(cacheRscPojoRef.getSuspend());
        cacheRscDataRef.resetIgnoreReasonsTo(cacheRscPojoRef.getIgnoreReasons());
    }

    @Override
    protected void removeCacheVlm(CacheRscData<Resource> cacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        cacheRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createCacheVlm(
        AbsVolume<Resource> vlmRef,
        CacheRscData<Resource> cacheRscDataRef,
        CacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
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
        String metaStorPoolNameStr = vlmPojo.getMetaStorPoolName();
        StorPool metaStorPool = null;
        if (metaStorPoolNameStr != null && !metaStorPoolNameStr.trim().isEmpty())
        {
            metaStorPool = vlmRef.getAbsResource().getNode().getStorPool(
                apiCtx,
                new StorPoolName(metaStorPoolNameStr)
            );
        }

        CacheVlmData<Resource> cacheVlmData = layerDataFactory.createCacheVlmData(
            vlmRef,
            cacheStorPool,
            metaStorPool,
            cacheRscDataRef
        );
        cacheRscDataRef.getVlmLayerObjects().put(vlmNrRef, cacheVlmData);
    }

    @Override
    protected void mergeCacheVlm(
        CacheVlmPojo vlmPojo,
        CacheVlmData<Resource> cacheVlmData
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
    protected BCacheRscData<Resource> createBCacheRscData(
        Resource rscRef,
        AbsRscLayerObject<Resource> parentRef,
        BCacheRscPojo bcacheRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        BCacheRscData<Resource> bcacheRscData = layerDataFactory.createBCacheRscData(
            bcacheRscPojoRef.getId(),
            rscRef,
            bcacheRscPojoRef.getRscNameSuffix(),
            parentRef
        );
        bcacheRscData.addAllIgnoreReasons(bcacheRscPojoRef.getIgnoreReasons());
        if (parentRef == null)
        {
            rscRef.setLayerData(apiCtx, bcacheRscData);
        }
        else
        {
            updateParent(bcacheRscData, parentRef);
        }
        return bcacheRscData;
    }

    @Override
    protected void mergeBCacheRscData(
        AbsRscLayerObject<Resource> parent,
        BCacheRscPojo bCacheRscPojo,
        BCacheRscData<Resource> bCacheRscData)
        throws AccessDeniedException, DatabaseException
    {
        bCacheRscData.setShouldSuspendIo(bCacheRscPojo.getSuspend());
        bCacheRscData.resetIgnoreReasonsTo(bCacheRscPojo.getIgnoreReasons());
    }

    @Override
    protected void removeBCacheVlm(BCacheRscData<Resource> bcacheRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        bcacheRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createBCacheVlm(
        AbsVolume<Resource> vlmRef,
        BCacheRscData<Resource> bcacheRscDataRef,
        BCacheVlmPojo vlmPojo,
        VolumeNumber vlmNrRef
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException
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

        BCacheVlmData<Resource> bcacheVlmData = layerDataFactory.createBCacheVlmData(
            vlmRef,
            cacheStorPool,
            bcacheRscDataRef
        );
        bcacheRscDataRef.getVlmLayerObjects().put(vlmNrRef, bcacheVlmData);
    }

    @Override
    protected void mergeBCacheVlm(
        BCacheVlmPojo vlmPojo,
        BCacheVlmData<Resource> bcacheVlmData
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
        // doing the following operations will be a no-op if old and new parent are the same object
        // however, those operations are still counted as modifications and can lead to a
        // concurrentModificationException
        // to prevent that, do not do those operations if the parent did not change
        if (!Objects.equals(newParent, oldParent))
        {
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
        else
        {
            if (newParent != null && !newParent.getChildren().contains(child))
            {
                newParent.getChildren().add(child);
            }
        }
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
        return layerDataFactory.createEbsData(
            vlmRef,
            storRscDataRef,
            storPoolRef
        );
    }

    @Override
    protected void mergeEbsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject<Resource> vlmDataRef)
        throws DatabaseException
    {
        // no-op
    }
}
