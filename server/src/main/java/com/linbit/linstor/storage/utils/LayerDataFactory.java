package com.linbit.linstor.storage.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
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
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
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
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
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
    private final ResourceLayerIdDatabaseDriver resourceLayerIdDatabaseDriver;

    private final LuksLayerDatabaseDriver luksDbDriver;
    private final DrbdLayerDatabaseDriver drbdDbDriver;
    private final StorageLayerDatabaseDriver storageDbDriver;
    private final NvmeLayerDatabaseDriver nvmeDbDriver;
    private final OpenflexLayerDatabaseDriver openflexDbDriver;
    private final WritecacheLayerDatabaseDriver writecacheDbDriver;
    private final CacheLayerDatabaseDriver cacheDbDriver;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public LayerDataFactory(
        ResourceLayerIdDatabaseDriver resourceLayerIdDatabaseDriverRef,
        LuksLayerDatabaseDriver luksDbDriverRef,
        DrbdLayerDatabaseDriver drbdDbDriverRef,
        StorageLayerDatabaseDriver storageDbDriverRef,
        NvmeLayerDatabaseDriver nvmeDbDriverRef,
        OpenflexLayerDatabaseDriver openflexDbDriverRef,
        WritecacheLayerDatabaseDriver writecacheDbDriverRef,
        CacheLayerDatabaseDriver cacheDbDriverRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        resourceLayerIdDatabaseDriver = resourceLayerIdDatabaseDriverRef;
        luksDbDriver = luksDbDriverRef;
        drbdDbDriver = drbdDbDriverRef;
        storageDbDriver = storageDbDriverRef;
        nvmeDbDriver = nvmeDbDriverRef;
        openflexDbDriver = openflexDbDriverRef;
        writecacheDbDriver = writecacheDbDriverRef;
        cacheDbDriver = cacheDbDriverRef;
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
            drbdDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(drbdRscData);
        drbdDbDriver.create(drbdRscData);
        return drbdRscData;
    }

    public <RSC extends AbsResource<RSC>> DrbdRscDfnData<RSC> createDrbdRscDfnData(
        ResourceName rscName,
        SnapshotName snapName,
        String resourceNameSuffix,
        short peerSlots,
        int alStripes,
        long alStripeSize,
        Integer portInt,
        TransportType transportType,
        String secret
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
            drbdDbDriver,
            transObjFactory,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdRscDfnData);
        return drbdRscDfnData;
    }

    public <RSC extends AbsResource<RSC>> DrbdVlmDfnData<RSC> createDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        ResourceName rscName,
        SnapshotName snapName,
        String resourceNameSuffix,
        VolumeNumber vlmNr,
        Integer minorNrInt,
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
            drbdDbDriver,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdVlmDfnData);
        return drbdVlmDfnData;
    }

    public <RSC extends AbsResource<RSC>> DrbdVlmData<RSC> createDrbdVlmData(
        AbsVolume<RSC> vlm,
        StorPool extMetaStorPool,
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
            drbdDbDriver,
            transObjFactory,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdVlmData);
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(disklessData);
        return disklessData;
    }

    public <RSC extends AbsResource<RSC>> LuksRscData<RSC> createLuksRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        AbsRscLayerObject<RSC> parentData
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
            luksDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(luksRscData);
        luksDbDriver.persist(luksRscData);
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
            luksDbDriver,
            transObjFactory,
            transMgrProvider
        );
        luksDbDriver.persist(luksVlmData);
        return luksVlmData;
    }

    public <RSC extends AbsResource<RSC>> StorageRscData<RSC> createStorageRscData(
        int rscLayerId,
        AbsRscLayerObject<RSC> parentRscData,
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(storageRscData);
        storageDbDriver.persist(storageRscData);
        return storageRscData;
    }

    public <RSC extends AbsResource<RSC>> NvmeRscData<RSC> createNvmeRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        AbsRscLayerObject<RSC> parentData
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
            nvmeDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(nvmeRscData);
        nvmeDbDriver.create(nvmeRscData);
        return nvmeRscData;
    }

    public <RSC extends AbsResource<RSC>> NvmeVlmData<RSC> createNvmeVlmData(
        AbsVolume<RSC> vlm,
        NvmeRscData<RSC> rscData
    )
    {
        return new NvmeVlmData<>(
            vlm,
            rscData,
            transObjFactory,
            transMgrProvider
        );
    }

    public <RSC extends AbsResource<RSC>> OpenflexRscDfnData<RSC> createOpenflexRscDfnData(
        ResourceName nameRef,
        String rscNameSuffixRef,
        String shortNameRef,
        String nqnRef
    )
        throws DatabaseException
    {
        OpenflexRscDfnData<RSC> ofRscDfnData = new OpenflexRscDfnData<>(
            nameRef,
            rscNameSuffixRef,
            shortNameRef,
            new ArrayList<>(),
            nqnRef,
            openflexDbDriver,
            transObjFactory,
            transMgrProvider
        );

        openflexDbDriver.create(ofRscDfnData);
        return ofRscDfnData;
    }

    public <RSC extends AbsResource<RSC>> OpenflexRscData<RSC> createOpenflexRscData(
        int rscLayerId,
        RSC rsc,
        OpenflexRscDfnData<RSC> rscDfnData,
        AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        OpenflexRscData<RSC> ofRscData = new OpenflexRscData<>(
            rscLayerId,
            rsc,
            rscDfnData,
            parentData,
            new HashSet<>(),
            new TreeMap<>(),
            openflexDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(ofRscData);
        openflexDbDriver.create(ofRscData);
        return ofRscData;
    }

    public <RSC extends AbsResource<RSC>> OpenflexVlmData<RSC> createOpenflexVlmData(
        AbsVolume<RSC> vlm,
        OpenflexRscData<RSC> rscData,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        OpenflexVlmData<RSC> ofTargetData = new OpenflexVlmData<>(
            vlm,
            rscData,
            storPoolRef,
            transObjFactory,
            transMgrProvider
        );
        openflexDbDriver.persist(ofTargetData);
        return ofTargetData;
    }

    public <RSC extends AbsResource<RSC>> WritecacheRscData<RSC> createWritecacheRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        WritecacheRscData<RSC> writecacheRscData = new WritecacheRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            rscNameSuffix,
            writecacheDbDriver,
            new TreeMap<>(),
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(writecacheRscData);
        writecacheDbDriver.persist(writecacheRscData);
        return writecacheRscData;
    }

    public <RSC extends AbsResource<RSC>> WritecacheVlmData<RSC> createWritecacheVlmData(
        AbsVolume<RSC> vlm,
        StorPool cacheStorPool,
        WritecacheRscData<RSC> rscData
    )
    {
        return new WritecacheVlmData<>(
            vlm,
            rscData,
            cacheStorPool,
            writecacheDbDriver,
            transObjFactory,
            transMgrProvider
        );
    }

    public <RSC extends AbsResource<RSC>> CacheRscData<RSC> createCacheRscData(
        int rscLayerId,
        RSC rsc,
        String rscNameSuffix,
        AbsRscLayerObject<RSC> parentData
    )
        throws DatabaseException
    {
        CacheRscData<RSC> cacheRscData = new CacheRscData<>(
            rscLayerId,
            rsc,
            parentData,
            new HashSet<>(),
            rscNameSuffix,
            cacheDbDriver,
            new TreeMap<>(),
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(cacheRscData);
        cacheDbDriver.persist(cacheRscData);
        return cacheRscData;
    }

    public <RSC extends AbsResource<RSC>> CacheVlmData<RSC> createCacheVlmData(
        AbsVolume<RSC> vlm,
        StorPool cacheStorPool,
        StorPool metaStorPool,
        CacheRscData<RSC> rscData
    )
    {
        return new CacheVlmData<>(
            vlm,
            rscData,
            cacheStorPool,
            metaStorPool,
            cacheDbDriver,
            transObjFactory,
            transMgrProvider
        );
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(lvmData);
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(lvmThinData);
        return lvmThinData;
    }

    public <RSC extends AbsResource<RSC>> SpdkData<RSC> createSpdkData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> rscData,
        StorPool storPoolRef
    )
        throws DatabaseException
    {
        SpdkData<RSC> spdkData = new SpdkData<>(
            vlm,
            rscData,
            storPoolRef,
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(spdkData);
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(zfsData);
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
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(fileData);
        return fileData;
    }
}
