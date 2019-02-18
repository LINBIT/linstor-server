package com.linbit.linstor.storage.utils;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.drdbdiskless.DrbdDisklessData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
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

    private final CryptSetupLayerDatabaseDriver cryptSetupDbDriver;
    private final DrbdLayerDatabaseDriver drbdDbDriver;
    private final StorageLayerDatabaseDriver storageDbDriver;
    private final SwordfishLayerDatabaseDriver swordfishDbDriver;

    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public LayerDataFactory(
        ResourceLayerIdDatabaseDriver resourceLayerIdDatabaseDriverRef,
        CryptSetupLayerDatabaseDriver cryptSetupDbDriverRef,
        DrbdLayerDatabaseDriver drbdDbDriverRef,
        StorageLayerDatabaseDriver storageDbDriverRef,
        SwordfishLayerDatabaseDriver swordfishDbDriverRef,

        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        resourceLayerIdDatabaseDriver = resourceLayerIdDatabaseDriverRef;
        cryptSetupDbDriver = cryptSetupDbDriverRef;
        drbdDbDriver = drbdDbDriverRef;
        storageDbDriver = storageDbDriverRef;
        swordfishDbDriver = swordfishDbDriverRef;

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
        Short peerSlots,
        Integer alStripes,
        Long alStripeSize,
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
        TcpPortNumber port,
        TransportType transportType,
        String secret
    )
        throws SQLException
    {
        DrbdRscDfnData drbdRscDfnData = new DrbdRscDfnData(
            rscDfn,
            resourceNameSuffix,
            peerSlots,
            alStripes,
            alStripeSize,
            port,
            transportType,
            secret,
            new ArrayList<>(),
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
        MinorNumber minorNr
    )
        throws SQLException
    {
        DrbdVlmDfnData drbdVlmDfnData = new DrbdVlmDfnData(
            vlmDfn,
            resourceNameSuffix,
            minorNr,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdVlmDfnData);
        return drbdVlmDfnData;
    }

    public DrbdVlmData createDrbdVlmData(
        Volume vlm,
        DrbdRscData rscData,
        DrbdVlmDfnData vlmDfnData
    )
        throws SQLException
    {
        DrbdVlmData drbdVlmData = new DrbdVlmData(
            vlm,
            rscData,
            vlmDfnData,
            transObjFactory,
            transMgrProvider
        );
        drbdDbDriver.persist(drbdVlmData);
        return drbdVlmData;
    }

    public DrbdDisklessData createDrbdDisklessData(
        Volume vlm,
        long usableSize,
        RscLayerObject rscData
    )
    {
        return new DrbdDisklessData(
            vlm,
            rscData,
            usableSize,
            transObjFactory,
            transMgrProvider
        );
    }

    public CryptSetupRscData createCryptSetupRscData(
        int rscLayerId,
        Resource rsc,
        String rscNameSuffix,
        RscLayerObject parentData
    )
        throws SQLException
    {
        CryptSetupRscData cryptSetupRscData = new CryptSetupRscData(
            rscLayerId,
            rsc,
            rscNameSuffix,
            parentData,
            new HashSet<>(),
            new TreeMap<>(),
            cryptSetupDbDriver,
            transObjFactory,
            transMgrProvider
        );
        resourceLayerIdDatabaseDriver.persist(cryptSetupRscData);
        cryptSetupDbDriver.persist(cryptSetupRscData);
        return cryptSetupRscData;
    }

    public CryptSetupVlmData createCryptSetupVlmData(
        Volume vlm,
        CryptSetupRscData rscData,
        byte[] password
    )
        throws SQLException
    {
        CryptSetupVlmData cryptSetupVlmData = new CryptSetupVlmData(
            vlm,
            rscData,
            password,
            cryptSetupDbDriver,
            transObjFactory,
            transMgrProvider
        );
        cryptSetupDbDriver.persist(cryptSetupVlmData);
        return cryptSetupVlmData;
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

    public LvmData createLvmData(Volume vlm, StorageRscData rscData) throws SQLException
    {
        LvmData lvmData = new LvmData(
            vlm,
            rscData,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(lvmData);
        return lvmData;
    }

    public LvmThinData createLvmThinData(Volume vlm, StorageRscData rscData) throws SQLException
    {
        LvmThinData lvmThinData = new LvmThinData(
            vlm,
            rscData,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(lvmThinData);
        return lvmThinData;
    }

    public SfInitiatorData createSfInitData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        SfVlmDfnData sfVlmDfnData
    )
        throws SQLException
    {
        SfInitiatorData sfInitiatorData = new SfInitiatorData(
            storRscDataRef,
            vlmRef,
            sfVlmDfnData,
            transObjFactory,
            transMgrProvider
        );
        swordfishDbDriver.persist(sfInitiatorData);
        return sfInitiatorData;
    }

    public SfTargetData createSfTargetData(
        Volume vlm,
        StorageRscData rscData,
        SfVlmDfnData sfVlmDfnData
    )
        throws SQLException
    {
        SfTargetData sfTargetData = new SfTargetData(
            vlm,
            rscData,
            sfVlmDfnData,
            transObjFactory,
            transMgrProvider
        );
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
        DeviceProviderKind kind
    )
        throws SQLException
    {
        ZfsData zfsData = new ZfsData(
            vlm,
            rscData,
            kind,
            transObjFactory,
            transMgrProvider
        );
        storageDbDriver.persist(zfsData);
        return zfsData;
    }
}
