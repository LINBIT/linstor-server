package com.linbit.linstor.storage;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupRscStltData;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupVlmStltData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.storage.layer.provider.diskless.DrbdDisklessVlmObjectData;
import com.linbit.linstor.storage.layer.provider.lvm.LvmData;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.layer.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.layer.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.layer.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsData;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;
import java.util.Map;

@Singleton
public class LayerDataFactory
{
    private final CryptSetupDatabaseDriver cryptSetupDbDriver;
    private final DrbdRscDfnDatabaseDriver drbdRscDfnDbDriver;
    private final DrbdVlmDfnDatabaseDriver drbdVlmDfnDbDriver;

    private final DrbdRscDatabaseDriver drbdRscDbDriver;
    //    private final DrbdVlmDatabaseDriver drbdVlmcDbDriver;
    //    private final LvmDatabaseDriver lvmDbDriver;
    //    private final LvmThinDatabaseDriver lvmThinDbDriver;
    //    private final RaidDatabaseDriver raidDbDriver;
    //    private final ZfsDatabaseDriver zfsDbDriver;
    //    private final ZfsThinDatabaseDriver zfsThinDbDriver;

    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public LayerDataFactory(
        CryptSetupDatabaseDriver cryptSetupDbDriverRef,
        DrbdRscDfnDatabaseDriver drbdRscDfnDbDriverRef,
        DrbdVlmDfnDatabaseDriver drbdVlmDfnDbDriverRef,

        DrbdRscDatabaseDriver drbdRscDbDriverRef,
        //        DrbdVlmDatabaseDriver drbdVlmcDbDriverRef,
        //        LvmDatabaseDriver lvmDbDriverRef,
        //        LvmThinDatabaseDriver lvmThinDbDriverRef,
        //        RaidDatabaseDriver raidDbDriverRef,
        //        ZfsDatabaseDriver zfsDbDriverRef,
        //        ZfsThinDatabaseDriver zfsThinDbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        cryptSetupDbDriver = cryptSetupDbDriverRef;
        drbdRscDfnDbDriver = drbdRscDfnDbDriverRef;
        drbdVlmDfnDbDriver = drbdVlmDfnDbDriverRef;

        drbdRscDbDriver = drbdRscDbDriverRef;
        //        drbdVlmcDbDriver = drbdVlmcDbDriverRef;
        //        lvmDbDriver = lvmDbDriverRef;
        //        lvmThinDbDriver = lvmThinDbDriverRef;
        //        raidDbDriver = raidDbDriverRef;
        //        zfsDbDriver = zfsDbDriverRef;
        //        zfsThinDbDriver = zfsThinDbDriverRef;

        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
    }

    public DrbdRscData createDrbdRscData(
        Resource rsc,
        String rscNameSuffix,
        @Nullable RscLayerObject parent,
        DrbdRscDfnData rscDfnData,
        List<RscLayerObject> childrenData,
        Map<VolumeNumber, DrbdVlmData> vlmDataMap,
        NodeId nodeId,
        boolean diskless,
        boolean disklessForPeers
    )
    {
        return new DrbdRscData(
            rsc,
            rscNameSuffix,
            parent,
            rscDfnData,
            childrenData,
            vlmDataMap,
            nodeId,
            disklessForPeers,
            diskless,
            transMgrProvider
        );
    }

    public DrbdRscDfnData createDrbdRscDfnData(
        ResourceDefinition rscDfn,
        TcpPortNumber port,
        TransportType transportType,
        String secret,
        List<DrbdRscData> drbdRscDataList
    )
    {
        return new DrbdRscDfnData(
            rscDfn,
            port,
            transportType,
            secret,
            drbdRscDataList,
            transMgrProvider
        );
    }

    public DrbdVlmDfnData createDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        MinorNumber minorNr,
        int peerSlots
    )
    {
        return new DrbdVlmDfnData(
            vlmDfn,
            minorNr,
            peerSlots,
            transMgrProvider
        );
    }

    public DrbdVlmData createDrbdVlmData(
        Volume vlm,
        DrbdRscData rscData,
        DrbdVlmDfnData vlmDfnData
    )
    {
        return new DrbdVlmData(
            vlm,
            rscData,
            vlmDfnData,
            transMgrProvider
        );
    }

    public DrbdDisklessVlmObjectData createDrbdDisklessData(
        Volume vlm,
        RscLayerObject rscData
    )
    {
        return new DrbdDisklessVlmObjectData(
            vlm,
            rscData,
            transMgrProvider
        );
    }

    public CryptSetupRscStltData createCryptSetupRscData(
        Resource rsc,
        String rscNameSuffix,
        RscLayerObject parentData,
        List<RscLayerObject> childrenDataList,
        Map<VolumeNumber, CryptSetupVlmStltData> vlmDataMap
    )
    {
        return new CryptSetupRscStltData(
            rsc,
            rscNameSuffix,
            parentData,
            childrenDataList,
            vlmDataMap,
            transMgrProvider
        );
    }

    public CryptSetupVlmStltData createCryptSetupVlmData(
        Volume vlm,
        CryptSetupRscStltData rscData,
        byte[] password
    )
    {
        return new CryptSetupVlmStltData(
            vlm,
            rscData,
            password,
            transMgrProvider
        );
    }

    public StorageRscData createStorageRscData(
        RscLayerObject parentRscData,
        Resource rsc,
        String rscNameSuffix,
        Map<VolumeNumber, VlmProviderObject> vlmDataMap
    )
    {
        return new StorageRscData(
            parentRscData,
            rsc,
            rscNameSuffix,
            vlmDataMap,
            transMgrProvider
        );
    }

    public LvmData createLvmData(Volume vlm, StorageRscData rscData)
    {
        return new LvmData(
            vlm,
            rscData,
            transMgrProvider
        );
    }

    public LvmThinData createLvmThinData(Volume vlm, StorageRscData rscData)
    {
        return new LvmThinData(
            vlm,
            rscData,
            transMgrProvider
        );
    }

    public SfInitiatorData createSfInitData(
        Volume vlmRef,
        StorageRscData storRscDataRef,
        SfVlmDfnData sfVlmDfnData
    )
    {
        return new SfInitiatorData(
            storRscDataRef,
            vlmRef,
            sfVlmDfnData,
            transMgrProvider
        );
    }

    public SfTargetData createSfTargetData(
        Volume vlm,
        StorageRscData rscData,
        SfVlmDfnData sfVlmDfnData
    )
    {
        return new SfTargetData(
            vlm,
            rscData,
            sfVlmDfnData,
            transMgrProvider
        );
    }

    public ZfsData createZfsData(
        Volume vlm,
        StorageRscData rscData,
        DeviceProviderKind kind
    )
    {
        return new ZfsData(
            vlm,
            rscData,
            kind,
            transMgrProvider
        );
    }
}
