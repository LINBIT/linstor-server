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
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishVlmDfnPojo;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolSatelliteFactory;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.Volume;
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
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StltLayerRscDataMerger extends AbsLayerRscDataMerger
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
    protected DrbdRscDfnData mergeOrCreateDrbdRscDfnData(
        ResourceDefinition rscDfn,
        DrbdRscDfnPojo drbdRscDfnPojo
    )
        throws IllegalArgumentException, DatabaseException, ValueOutOfRangeException, AccessDeniedException,
        ExhaustedPoolException, ValueInUseException
    {
        DrbdRscDfnData rscDfnData = rscDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdRscDfnPojo.getRscNameSuffix()
        );
        if (rscDfnData == null)
        {
            rscDfnData = layerDataFactory.createDrbdRscDfnData(
                rscDfn,
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
    protected DrbdRscData createDrbdRscData(
        Resource rsc, RscLayerDataApi rscDataPojo, RscLayerObject parent, DrbdRscPojo drbdRscPojo,
        DrbdRscDfnData drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, AccessDeniedException
    {
        DrbdRscData drbdRscData;
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
    protected void mergeDrbdRscData(RscLayerObject parentRef, DrbdRscPojo drbdRscPojoRef, DrbdRscData drbdRscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.getFlags().resetFlagsTo(
            apiCtx,
            DrbdRscFlags.restoreFlags(drbdRscPojoRef.getFlags())
        );
        updateParent(drbdRscDataRef, parentRef);
    }

    @Override
    protected void removeDrbdVlm(DrbdRscData drbdRscDataRef, VolumeNumber vlmNrRef)
        throws AccessDeniedException, DatabaseException
    {
        drbdRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected DrbdVlmDfnData mergeOrCreateDrbdVlmDfnData(VolumeDefinition vlmDfnRef, DrbdVlmDfnPojo drbdVlmDfnPojoRef)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        DrbdVlmDfnData drbdVlmDfnData = vlmDfnRef.getLayerData(
            apiCtx,
            DeviceLayerKind.DRBD,
            drbdVlmDfnPojoRef.getRscNameSuffix()
        );
        if (drbdVlmDfnData == null)
        {
            drbdVlmDfnData = layerDataFactory.createDrbdVlmDfnData(
                vlmDfnRef,
                drbdVlmDfnPojoRef.getRscNameSuffix(),
                drbdVlmDfnPojoRef.getMinorNr(),
                vlmDfnRef.getResourceDefinition().getLayerData(
                    apiCtx,
                    DeviceLayerKind.DRBD,
                    drbdVlmDfnPojoRef.getRscNameSuffix()
                )
            );
            vlmDfnRef.setLayerData(apiCtx, drbdVlmDfnData);
        }
        else
        {
            // nothing to merge
        }
        return drbdVlmDfnData;
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

        StorPool extMetaStorPool = null;
        String extMetaStorPoolNameStr = vlmPojoRef.getExternalMetaDataStorPool();
        if (extMetaStorPoolNameStr != null)
        {
            extMetaStorPool = vlmRef.getResource().getAssignedNode().getStorPool(
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
    protected LuksRscData createLuksRscData(Resource rscRef, RscLayerObject parentRef, LuksRscPojo luksRscPojoRef)
        throws DatabaseException, AccessDeniedException
    {
        LuksRscData luksRscData;
        luksRscData = layerDataFactory.createLuksRscData(
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
    protected void removeLuksVlm(LuksRscData luksRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        luksRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createOrMergeLuksVlm(Volume vlmRef, LuksRscData luksRscDataRef, LuksVlmPojo vlmPojoRef)
        throws DatabaseException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        VolumeNumber vlmNr = vlmDfn.getVolumeNumber();

        LuksVlmData luksVlmData = luksRscDataRef.getVlmLayerObjects().get(vlmNr);
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
    protected StorageRscData createStorageRscData(
        Resource rscRef, RscLayerObject parentRef, StorageRscPojo storRscPojoRef
    )
        throws DatabaseException, AccessDeniedException
    {
        StorageRscData storRscData;
        storRscData = layerDataFactory.createStorageRscData(
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
    protected void removeStorageVlm(StorageRscData storRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        storRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected StorPool getStoragePool(
        Volume vlm,
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
                    vlm.getResource().getAssignedNode(),
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
    protected VlmProviderObject createDisklessVlmData(
        Volume vlmRef, StorageRscData storRscDataRef, VlmLayerDataApi vlmPojoRef, StorPool storPoolRef
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
    protected void mergeDisklessVlm(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject createLvmVlmData(Volume vlmRef, StorageRscData storRscDataRef, StorPool storPoolRef)
        throws DatabaseException
    {
        return layerDataFactory.createLvmData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeLvmVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected LvmThinData createLvmThinVlmData(Volume vlmRef, StorageRscData storRscDataRef, StorPool storPoolRef)
        throws DatabaseException
    {
        return layerDataFactory.createLvmThinData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected VlmProviderObject createSpdkVlmData(Volume vlmRef, StorageRscData storRscDataRef, StorPool storPoolRef)
            throws DatabaseException {
        return layerDataFactory.createSpdkData(vlmRef, storRscDataRef, storPoolRef);
    }

    @Override
    protected void mergeSpdkVlmData(VlmLayerDataApi vlmPojo, VlmProviderObject vlmData) throws DatabaseException {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected void mergeLvmThinVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
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
        VlmProviderObject vlmData;
        vlmData = layerDataFactory.createSfInitData(
            vlmRef,
            storRscDataRef,
            restoreSfVlmDfn(
                vlmRef.getVolumeDefinition(),
                ((SwordfishInitiatorVlmPojo) vlmPojoRef).getVlmDfn()
            ),
            storPoolRef
        );
        return vlmData;
    }

    @Override
    protected void mergeSfInitVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject createSfTargetVlmData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException, AccessDeniedException
    {
        VlmProviderObject vlmData;
        vlmData = layerDataFactory.createSfTargetData(
            vlmRef,
            storRscDataRef,
            restoreSfVlmDfn(
                vlmRef.getVolumeDefinition(),
                ((SwordfishInitiatorVlmPojo) vlmPojoRef).getVlmDfn()
            ),
            storPoolRef
        );
        return vlmData;
    }

    @Override
    protected void mergeSfTargetVlmData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
    }

    @Override
    protected VlmProviderObject createZfsData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createZfsData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeZfsData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected VlmProviderObject createFileData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        VlmLayerDataApi vlmPojoRef,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        return layerDataFactory.createFileData(vlmRef, storRscDataRef, vlmPojoRef.getProviderKind(), storPoolRef);
    }

    @Override
    protected void mergeFileData(VlmLayerDataApi vlmPojoRef, VlmProviderObject vlmDataRef) throws DatabaseException
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring usableSize
    }

    @Override
    protected void setStorPool(VlmProviderObject vlmDataRef, StorPool storPoolRef) throws AccessDeniedException, DatabaseException
    {
        vlmDataRef.setStorPool(apiCtx, storPoolRef);
    }

    @Override
    protected void putVlmData(StorageRscData storRscDataRef, VlmProviderObject vlmDataRef)
    {
        storRscDataRef.getVlmLayerObjects().put(vlmDataRef.getVlmNr(), vlmDataRef);
    }

    @Override
    protected NvmeRscData createNvmeRscData(Resource rscRef, RscLayerObject parentRef, NvmeRscPojo nvmeRscPojoRef)
        throws DatabaseException, AccessDeniedException
    {
        NvmeRscData nvmeRscData;
        nvmeRscData = layerDataFactory.createNvmeRscData(
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
    protected void removeNvmeVlm(NvmeRscData nvmeRscDataRef, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        nvmeRscDataRef.remove(apiCtx, vlmNrRef);
    }

    @Override
    protected void createNvmeVlm(Volume vlmRef, NvmeRscData nvmeRscDataRef, VolumeNumber vlmNrRef)
    {
        NvmeVlmData nvmeVlmData;
        nvmeVlmData = layerDataFactory.createNvmeVlmData(vlmRef, nvmeRscDataRef);
        nvmeRscDataRef.getVlmLayerObjects().put(vlmNrRef, nvmeVlmData);
    }

    @Override
    protected void mergeNvmeVlm(NvmeVlmPojo vlmPojoRef, NvmeVlmData nvmeVlmDataRef)
    {
        // ignoring allocatedSize
        // ignoring devicePath
        // ignoring diskState
        // ignoring usableSize
    }

    @Override
    protected void updateParent(RscLayerObject child, RscLayerObject newParent) throws DatabaseException
    {
        RscLayerObject oldParent = child.getParent();
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

    private SfVlmDfnData restoreSfVlmDfn(
        VolumeDefinition vlmDfn,
        SwordfishVlmDfnPojo vlmDfnPojo
    )
        throws AccessDeniedException, DatabaseException
    {
        SfVlmDfnData sfVlmDfnData;
        VlmDfnLayerObject vlmDfnLayerData = vlmDfn.getLayerData(
            apiCtx,
            DeviceLayerKind.STORAGE,
            vlmDfnPojo.getRscNameSuffix()
        );
        if (vlmDfnLayerData == null)
        {
            sfVlmDfnData = layerDataFactory.createSfVlmDfnData(
                vlmDfn,
                vlmDfnPojo.getVlmOdata(),
                vlmDfnPojo.getRscNameSuffix()
            );
            vlmDfn.setLayerData(apiCtx, sfVlmDfnData);
        }
        else
        {
            if (!(vlmDfnLayerData instanceof SfVlmDfnData))
            {
                throw new ImplementationError(
                    "Unexpected VolumeDefinition layer object. Swordfish expected, but got " +
                        vlmDfnLayerData.getClass().getSimpleName()
                );
            }
            sfVlmDfnData = (SfVlmDfnData) vlmDfnLayerData;
            sfVlmDfnData.setVlmOdata(vlmDfnPojo.getVlmOdata());
        }
        return sfVlmDfnData;
    }
}
