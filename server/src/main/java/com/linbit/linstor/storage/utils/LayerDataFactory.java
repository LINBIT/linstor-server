package com.linbit.linstor.storage.utils;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
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
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
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
    private final SwordfishLayerDatabaseDriver swordfishDbDriver;
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
        SwordfishLayerDatabaseDriver swordfishDbDriverRef,
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
        swordfishDbDriver = swordfishDbDriverRef;
        tcpPortPool = tcpPortPoolRef;
        minorPool = minorPoolRef;

        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
    }

    public DrbdRscData createDrbdRscData(
        int rscLayerId,
        Resource rsc,
        String rscNameSuffix,
        @Nullable RscLayerObject parent,
        DrbdRscDfnData rscDfnData,
        NodeId nodeId,
        @Nullable Short peerSlots,
        @Nullable Integer alStripes,
        @Nullable Long alStripeSize,
        long initFlags
    )
        throws SQLException
    {
        DrbdRscData drbdRscData = new DrbdRscData(
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

    public DrbdRscDfnData createDrbdRscDfnData(
        ResourceDefinition rscDfn,
        String resourceNameSuffix,
        short peerSlots,
        int alStripes,
        long alStripeSize,
        Integer portInt,
        TransportType transportType,
        String secret
    )
        throws SQLException, ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        DrbdRscDfnData drbdRscDfnData = new DrbdRscDfnData(
            rscDfn,
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

    public DrbdVlmDfnData createDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        String resourceNameSuffix,
        Integer minorNrInt,
        DrbdRscDfnData drbdRscDfnData
    )
        throws SQLException, ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        DrbdVlmDfnData drbdVlmDfnData = new DrbdVlmDfnData(
            vlmDfn,
            resourceNameSuffix,
            minorNrInt,
            minorPool,
            drbdRscDfnData,
            drbdDbDriver,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdVlmDfnData);
        return drbdVlmDfnData;
    }

    public DrbdVlmData createDrbdVlmData(
        Volume vlm,
        StorPool extMetaStorPool,
        DrbdRscData rscData,
        DrbdVlmDfnData vlmDfnData
    )
        throws SQLException
    {
        DrbdVlmData drbdVlmData = new DrbdVlmData(
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

    public DisklessData createDisklessData(
        Volume vlm,
        long usableSize,
        StorageRscData rscData,
        StorPool storPoolRef
    )
        throws SQLException
    {
        DisklessData disklessData = new DisklessData(
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

    public LuksRscData createLuksRscData(
        int rscLayerId,
        Resource rsc,
        String rscNameSuffix,
        RscLayerObject parentData
    )
        throws SQLException
    {
        LuksRscData luksRscData = new LuksRscData(
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

    public LuksVlmData createLuksVlmData(
        Volume vlm,
        LuksRscData rscData,
        byte[] password
    )
        throws SQLException
    {
        LuksVlmData luksVlmData = new LuksVlmData(
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

    public StorageRscData createStorageRscData(
        int rscLayerId,
        RscLayerObject parentRscData,
        Resource rsc,
        String rscNameSuffix
    )
        throws SQLException
    {
        StorageRscData storageRscData = new StorageRscData(
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

    public NvmeRscData createNvmeRscData(
        int rscLayerId,
        Resource rsc,
        String rscNameSuffix,
        RscLayerObject parentData
    )
        throws SQLException
    {
        NvmeRscData nvmeRscData = new NvmeRscData(
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

    public NvmeVlmData createNvmeVlmData(Volume vlm, NvmeRscData rscData)
    {
        return new NvmeVlmData(
            vlm,
            rscData,
            transObjFactory,
            transMgrProvider
        );
    }

    public LvmData createLvmData(
        Volume vlm,
        StorageRscData rscData,
        StorPool storPoolRef
    )
        throws SQLException
    {
        LvmData lvmData = new LvmData(
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

    public LvmThinData createLvmThinData(
        Volume vlm,
        StorageRscData rscData,
        StorPool storPoolRef
    )
        throws SQLException
    {
        LvmThinData lvmThinData = new LvmThinData(
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

    public SfInitiatorData createSfInitData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        SfVlmDfnData sfVlmDfnData,
        StorPool storPoolRef
    )
        throws SQLException
    {
        SfInitiatorData sfInitiatorData = new SfInitiatorData(
            storRscDataRef,
            vlmRef,
            sfVlmDfnData,
            storPoolRef,
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(sfInitiatorData);
        swordfishDbDriver.persist(sfInitiatorData);
        return sfInitiatorData;
    }

    public SfTargetData createSfTargetData(
        Volume vlm,
        StorageRscData rscData,
        SfVlmDfnData sfVlmDfnData,
        StorPool storPoolRef
    )
        throws SQLException
    {
        SfTargetData sfTargetData = new SfTargetData(
            vlm,
            rscData,
            sfVlmDfnData,
            storPoolRef,
            storageDbDriver,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(sfTargetData);
        swordfishDbDriver.persist(sfTargetData);
        return sfTargetData;
    }

    public SfVlmDfnData createSfVlmDfnData(
        VolumeDefinition volumeDefinitionRef,
        String vlmOdata,
        String suffixedResourceName
    )
        throws SQLException
    {
        SfVlmDfnData sfVlmDfnData = new SfVlmDfnData(
            volumeDefinitionRef,
            vlmOdata,
            suffixedResourceName,
            swordfishDbDriver,
            transObjFactory,
            transMgrProvider
        );
        swordfishDbDriver.persist(sfVlmDfnData);
        return sfVlmDfnData;
    }


    public ZfsData createZfsData(
        Volume vlm,
        StorageRscData rscData,
        DeviceProviderKind kind,
        StorPool storPoolRef
    )
        throws SQLException
    {
        ZfsData zfsData = new ZfsData(
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

    public FileData createFileData(Volume vlm, StorageRscData rscData, DeviceProviderKind kind, StorPool storPool)
        throws SQLException
    {
        FileData fileData = new FileData(
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
