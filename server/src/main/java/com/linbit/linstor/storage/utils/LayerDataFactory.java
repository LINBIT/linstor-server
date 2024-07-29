package com.linbit.linstor.storage.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheVlmDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
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
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

@Singleton
public class LayerDataFactory
{
    private final LayerResourceIdDatabaseDriver layerRscIdDatabaseDriver;

    private final LayerDrbdRscDfnDatabaseDriver layerDrbdRscDfnDbDriver;
    private final LayerDrbdVlmDfnDatabaseDriver layerDrbdVlmDfnDbDriver;
    private final LayerDrbdRscDatabaseDriver layerDrbdRscDbDriver;
    private final LayerDrbdVlmDatabaseDriver layerDrbdVlmDbDriver;

    private final LayerLuksRscDatabaseDriver layerLuksRscDbDriver;
    private final LayerLuksVlmDatabaseDriver layerLuksVlmDbDriver;

    private final LayerStorageRscDatabaseDriver layerStorRscDbDriver;
    private final LayerStorageVlmDatabaseDriver layerStorVlmDbDriver;

    private final LayerNvmeRscDatabaseDriver layerNvmeRscDbDriver;

    private final LayerWritecacheRscDatabaseDriver layerWritecacheRscDbDriver;
    private final LayerWritecacheVlmDatabaseDriver layerWritecacheVlmDbDriver;

    private final LayerCacheRscDatabaseDriver layerCacheRscDbDriver;
    private final LayerCacheVlmDatabaseDriver layerCacheVlmDbDriver;

    private final LayerBCacheRscDatabaseDriver layerBCacheRscDbDriver;
    private final LayerBCacheVlmDatabaseDriver layerBCacheVlmDbDriver;

    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;


    @Inject
    public LayerDataFactory(
        LayerResourceIdDatabaseDriver layerRscIdDatabaseDriverRef,
        LayerLuksRscDatabaseDriver layerLuksRscDbDriverRef,
        LayerLuksVlmDatabaseDriver layerLuksVlmDbDriverRef,
        LayerDrbdRscDfnDatabaseDriver layerDrbdRscDfnDbDriverRef,
        LayerDrbdVlmDfnDatabaseDriver layerDrbdVlmDfnDbDriverRef,
        LayerDrbdRscDatabaseDriver layerDrbdRscDbDriverRef,
        LayerDrbdVlmDatabaseDriver layerDrbdVlmDbDriverRef,
        LayerStorageRscDatabaseDriver layerStorRscDbDriverRef,
        LayerStorageVlmDatabaseDriver layerStorVlmDbDriverRef,
        LayerNvmeRscDatabaseDriver layerNvmeRscDbDriverRef,
        LayerWritecacheRscDatabaseDriver layerWritecacheRscDbDriverRef,
        LayerWritecacheVlmDatabaseDriver layerWritecacheVlmDbDriverRef,
        LayerCacheRscDatabaseDriver layerCacheRscDbDriverRef,
        LayerCacheVlmDatabaseDriver layerCacheVlmDbDriverRef,
        LayerBCacheRscDatabaseDriver layerBCacheRscDbDriverRef,
        LayerBCacheVlmDatabaseDriver layerBCacheVlmDbDriverRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        layerRscIdDatabaseDriver = layerRscIdDatabaseDriverRef;
        layerLuksRscDbDriver = layerLuksRscDbDriverRef;
        layerLuksVlmDbDriver = layerLuksVlmDbDriverRef;
        layerDrbdRscDfnDbDriver = layerDrbdRscDfnDbDriverRef;
        layerDrbdVlmDfnDbDriver = layerDrbdVlmDfnDbDriverRef;
        layerDrbdRscDbDriver = layerDrbdRscDbDriverRef;
        layerDrbdVlmDbDriver = layerDrbdVlmDbDriverRef;
        layerStorRscDbDriver = layerStorRscDbDriverRef;
        layerStorVlmDbDriver = layerStorVlmDbDriverRef;
        layerNvmeRscDbDriver = layerNvmeRscDbDriverRef;
        layerWritecacheRscDbDriver = layerWritecacheRscDbDriverRef;
        layerWritecacheVlmDbDriver = layerWritecacheVlmDbDriverRef;
        layerCacheRscDbDriver = layerCacheRscDbDriverRef;
        layerCacheVlmDbDriver = layerCacheVlmDbDriverRef;
        layerBCacheRscDbDriver = layerBCacheRscDbDriverRef;
        layerBCacheVlmDbDriver = layerBCacheVlmDbDriverRef;
        tcpPortPool = tcpPortPoolRef;
        minorPool = minorPoolRef;

        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
    }

    public <RSC extends AbsResource<RSC>> DrbdRscData<RSC> createDrbdRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parent,
        DrbdRscDfnData<RSC> rscDfnData,
        NodeId nodeId,
        @Nullable Short peerSlots,
        @Nullable Integer alStripes,
        @Nullable Long alStripeSize,
        long initFlags
    )
        throws DatabaseException
    {
        // check that the nodeid is unique within rscdatalist
        // there is somewhere a race condition and we couldn't determine the cause yet
        // for (DrbdRscData<RSC> sib : rscDfnData.getDrbdRscDataList())
        // {
        // if (sib.getNodeId().equals(nodeId))
        // {
        // final String allIds = rscDfnData.getDrbdRscDataList().stream()
        // .map(rscData -> Integer.toString(rscData.getNodeId().value))
        // .collect(Collectors.joining(","));
        // throw new LinStorRuntimeException(
        // String.format("Duplicate node id '%d' detected. ids: [%s]", nodeId.value, allIds)
        // );
        // }
        // }
        DrbdRscData<RSC> drbdRscData = new DrbdRscData<>(
            rscLayerId,
            rsc,
            parent,
            rscDfnData,
            new HashSet<>(),
            new TreeMap<>(),
            rscNameSuffix,
            nodeId,
            peerSlots,
            alStripes,
            alStripeSize,
            initFlags,
            layerDrbdRscDbDriver,
            layerDrbdVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(drbdRscData);
        layerDrbdRscDbDriver.create(drbdRscData);
        return drbdRscData;
    }

    public <RSC extends AbsResource<RSC>> DrbdRscDfnData<RSC> createDrbdRscDfnData(
        ResourceName rscName,
        @Nullable SnapshotName snapName,
        String resourceNameSuffix,
        short peerSlots,
        int alStripes,
        long alStripeSize,
        Integer portInt,
        TransportType transportType,
        @Nullable String secret
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        DrbdRscDfnData<RSC> drbdRscDfnData = new DrbdRscDfnData<>(
            rscName,
            snapName,
            resourceNameSuffix,
            peerSlots,
            alStripes,
            alStripeSize,
            portInt,
            transportType,
            secret,
            new ArrayList<>(),
            new TreeMap<>(),
            tcpPortPool,
            layerDrbdRscDfnDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerDrbdRscDfnDbDriver.create(drbdRscDfnData);
        return drbdRscDfnData;
    }

    public <RSC extends AbsResource<RSC>> DrbdVlmDfnData<RSC> createDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        ResourceName rscName,
        @Nullable SnapshotName snapName,
        String resourceNameSuffix,
        VolumeNumber vlmNr,
        @Nullable Integer minorNrInt,
        DrbdRscDfnData<RSC> drbdRscDfnData
    )
        throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        DrbdVlmDfnData<RSC> drbdVlmDfnData = new DrbdVlmDfnData<>(
            vlmDfn,
            rscName,
            snapName,
            resourceNameSuffix,
            vlmNr,
            minorNrInt,
            minorPool,
            drbdRscDfnData,
            layerDrbdVlmDfnDbDriver,
            transMgrProvider
        );
        layerDrbdVlmDfnDbDriver.create(drbdVlmDfnData);
        return drbdVlmDfnData;
    }

    public <RSC extends AbsResource<RSC>> DrbdVlmData<RSC> createDrbdVlmData(
        AbsVolume<RSC> vlm,
        @Nullable StorPool extMetaStorPool,
        DrbdRscData<RSC> rscData,
        DrbdVlmDfnData<RSC> vlmDfnData
    )
        throws DatabaseException
    {
        DrbdVlmData<RSC> drbdVlmData = new DrbdVlmData<>(
            vlm,
            rscData,
            vlmDfnData,
            extMetaStorPool,
            layerDrbdVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerDrbdVlmDbDriver.create(drbdVlmData);
        return drbdVlmData;
    }

    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> DisklessData<RSC> createDisklessData(
        VLM vlm,
        long usableSize,
        StorageRscData<RSC> rscData,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        DisklessData<RSC> disklessData = new DisklessData<>(
            vlm,
            rscData,
            usableSize,
            storPoolRef,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(disklessData);
        return disklessData;
    }

    public <RSC extends AbsResource<RSC>> LuksRscData<RSC> createLuksRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        LuksRscData<RSC> luksRscData = new LuksRscData<>(
            rscLayerId,
            rsc,
            rscNameSuffix,
            parentData,
            new HashSet<>(),
            new TreeMap<>(),
            layerLuksRscDbDriver,
            layerLuksVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(luksRscData);
        layerLuksRscDbDriver.create(luksRscData);
        return luksRscData;
    }

    public <RSC extends AbsResource<RSC>> LuksVlmData<RSC> createLuksVlmData(
        AbsVolume<RSC> vlm,
        LuksRscData<RSC> rscData,
        byte[] password
    )
        throws DatabaseException
    {
        LuksVlmData<RSC> luksVlmData = new LuksVlmData<>(
            vlm,
            rscData,
            password,
            layerLuksVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerLuksVlmDbDriver.create(luksVlmData);
        return luksVlmData;
    }

    public <RSC extends AbsResource<RSC>> StorageRscData<RSC> createStorageRscData(
        int rscLayerId,
        @Nullable AbsRscLayerObject<RSC> parentRscData,
        RSC rsc,
        String rscNameSuffix
    )
        throws DatabaseException
    {
        StorageRscData<RSC> storageRscData = new StorageRscData<>(
            rscLayerId,
            parentRscData,
            rsc,
            rscNameSuffix,
            new TreeMap<>(),
            layerStorRscDbDriver,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(storageRscData);
        layerStorRscDbDriver.create(storageRscData);
        return storageRscData;
    }

    public <RSC extends AbsResource<RSC>> NvmeRscData<RSC> createNvmeRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        NvmeRscData<RSC> nvmeRscData = new NvmeRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            new TreeMap<>(),
            rscNameSuffix,
            layerNvmeRscDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(nvmeRscData);
        layerNvmeRscDbDriver.create(nvmeRscData);
        return nvmeRscData;
    }

    public <RSC extends AbsResource<RSC>> NvmeVlmData<RSC> createNvmeVlmData(
        AbsVolume<RSC> vlm,
        NvmeRscData<RSC> rscData
    )
    {
        // no LayerNvmeVolumes table right now
        return new NvmeVlmData<>(
            vlm,
            rscData,
            transObjFactory,
            transMgrProvider
        );
    }

    public <RSC extends AbsResource<RSC>> WritecacheRscData<RSC> createWritecacheRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        WritecacheRscData<RSC> writecacheRscData = new WritecacheRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            rscNameSuffix,
            layerWritecacheRscDbDriver,
            layerWritecacheVlmDbDriver,
            new TreeMap<>(),
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(writecacheRscData);
        layerWritecacheRscDbDriver.create(writecacheRscData);
        return writecacheRscData;
    }

    public <RSC extends AbsResource<RSC>> WritecacheVlmData<RSC> createWritecacheVlmData(
        AbsVolume<RSC> vlm,
        @Nullable StorPool cacheStorPool,
        WritecacheRscData<RSC> rscData
    )
        throws DatabaseException
    {
        WritecacheVlmData<RSC> writecacheVlmData = new WritecacheVlmData<>(
            vlm,
            rscData,
            cacheStorPool,
            transObjFactory,
            transMgrProvider
        );
        layerWritecacheVlmDbDriver.create(writecacheVlmData);
        return writecacheVlmData;
    }

    public <RSC extends AbsResource<RSC>> CacheRscData<RSC> createCacheRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        CacheRscData<RSC> cacheRscData = new CacheRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            rscNameSuffix,
            layerCacheRscDbDriver,
            layerCacheVlmDbDriver,
            new TreeMap<>(),
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(cacheRscData);
        layerCacheRscDbDriver.create(cacheRscData);
        return cacheRscData;
    }

    public <RSC extends AbsResource<RSC>> CacheVlmData<RSC> createCacheVlmData(
        AbsVolume<RSC> vlm,
        @Nullable StorPool cacheStorPool,
        @Nullable StorPool metaStorPool,
        CacheRscData<RSC> rscData
    )
        throws DatabaseException
    {
        CacheVlmData<RSC> cacheVlmData = new CacheVlmData<>(
            vlm,
            rscData,
            cacheStorPool,
            metaStorPool,
            transObjFactory,
            transMgrProvider
        );
        layerCacheVlmDbDriver.create(cacheVlmData);
        return cacheVlmData;
    }

    public <RSC extends AbsResource<RSC>> BCacheRscData<RSC> createBCacheRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        @Nullable AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        BCacheRscData<RSC> bcacheRscData = new BCacheRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            rscNameSuffix,
            layerBCacheRscDbDriver,
            layerBCacheVlmDbDriver,
            new TreeMap<>(),
            transObjFactory,
            transMgrProvider
        );
        layerRscIdDatabaseDriver.create(bcacheRscData);
        layerBCacheRscDbDriver.create(bcacheRscData);
        return bcacheRscData;
    }

    public <RSC extends AbsResource<RSC>> BCacheVlmData<RSC> createBCacheVlmData(
        AbsVolume<RSC> vlm,
        @Nullable StorPool cacheStorPool,
        BCacheRscData<RSC> rscData
    )
        throws DatabaseException
    {
        BCacheVlmData<RSC> bCacheVlmData = new BCacheVlmData<>(
            vlm,
            rscData,
            cacheStorPool,
            layerBCacheVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerBCacheVlmDbDriver.create(bCacheVlmData);
        return bCacheVlmData;
    }

    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> LvmData<RSC> createLvmData(
        VLM vlm,
        StorageRscData<RSC> rscData,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        LvmData<RSC> lvmData = new LvmData<>(
            vlm,
            rscData,
            storPoolRef,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(lvmData);
        return lvmData;
    }

    public <RSC extends AbsResource<RSC>> LvmThinData<RSC> createLvmThinData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> rscData,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        LvmThinData<RSC> lvmThinData = new LvmThinData<>(
            vlm,
            rscData,
            storPoolRef,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(lvmThinData);
        return lvmThinData;
    }

    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> StorageSpacesData<RSC> createStorageSpacesData(
        VLM vlm,
        StorageRscData<RSC> rscData,
        DeviceProviderKind kind,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        StorageSpacesData<RSC> storageSpacesData = new StorageSpacesData<>(
            vlm,
            rscData,
            kind,
            storPoolRef,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(storageSpacesData);
        return storageSpacesData;
    }

    public <RSC extends AbsResource<RSC>> SpdkData<RSC> createSpdkData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> rscData,
        DeviceProviderKind kind,
        StorPool storPool
    )
        throws DatabaseException
    {
        SpdkData<RSC> spdkData = new SpdkData<>(
            vlm,
            rscData,
            kind,
            storPool,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(spdkData);
        return spdkData;
    }

    public <RSC extends AbsResource<RSC>> ZfsData<RSC> createZfsData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> rscData,
        DeviceProviderKind kind,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        ZfsData<RSC> zfsData = new ZfsData<>(
            vlm,
            rscData,
            kind,
            storPoolRef,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(zfsData);
        return zfsData;
    }

    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> FileData<RSC> createFileData(
        VLM vlm,
        StorageRscData<RSC> rscData,
        DeviceProviderKind kind,
        StorPool storPool
    )
        throws DatabaseException
    {
        FileData<RSC> fileData = new FileData<>(
            vlm,
            rscData,
            kind,
            storPool,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(fileData);
        return fileData;
    }

    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> ExosData<RSC> createExosData(
        VLM vlm,
        StorageRscData<RSC> rscData,
        StorPool storPool
    )
        throws DatabaseException
    {
        ExosData<RSC> exosData = new ExosData<>(
            vlm,
            rscData,
            storPool,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(exosData);
        return exosData;
    }

    /**
     *
     * @param vlm
     * @param rscData
     * @param storPool
     * @param ebsVlmId can be null for target (will be filled after creation on special satellite)
     *
     * @return
     *
     * @throws DatabaseException
     */
    public <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> EbsData<RSC> createEbsData(
        VLM vlm,
        StorageRscData<RSC> rscData,
        StorPool storPool
    )
        throws DatabaseException
    {
        EbsData<RSC> ebsData = new EbsData<>(
            vlm,
            rscData,
            storPool.getDeviceProviderKind(),
            storPool,
            layerStorVlmDbDriver,
            transObjFactory,
            transMgrProvider
        );
        layerStorVlmDbDriver.create(ebsData);
        return ebsData;
    }
}
