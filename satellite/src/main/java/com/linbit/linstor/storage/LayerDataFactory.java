package com.linbit.linstor.storage;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupStltData;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscDfnDataStlt;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmDataStlt;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdRscDataStlt;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdVlmDfnDataStlt;
import com.linbit.linstor.storage2.layer.data.CryptSetupData;
import com.linbit.linstor.storage2.layer.kinds.CryptSetupLayerKind;
import com.linbit.linstor.storage2.layer.kinds.DrbdLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

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

    private final DrbdLayerKind drbdKind;
    private final CryptSetupLayerKind cryptKind;

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

        drbdKind = (DrbdLayerKind) ResourceType.DRBD.getDevLayerKind();
        cryptKind = (CryptSetupLayerKind) ResourceType.CRYPT.getDevLayerKind();
    }

    public DrbdRscDataStlt createDrbdRscData(
        Resource rsc,
        String identifier,
        NodeId nodeId,
        boolean diskless,
        boolean disklessForPeers
    )
    {
        return new DrbdRscDataStlt(
            nodeId,
            diskless,
            disklessForPeers
        );
    }

    public DrbdRscDfnDataStlt createDrbdRscDfnData(
        ResourceDefinition rscDfn,
        String identifier,
        TcpPortNumber port,
        TransportType transportType,
        String secret
    )
    {
        return new DrbdRscDfnDataStlt(
            port,
            transportType,
            secret,
            transMgrProvider
        );
    }

    public DrbdVlmDfnDataStlt createDrbdVlmDfnData(
        VolumeDefinition vlmDfn,
        MinorNumber minorNr,
        int peerSlots
    )
    {
        return new DrbdVlmDfnDataStlt(
            minorNr,
            peerSlots,
            transMgrProvider
        );
    }

    public DrbdVlmDataStlt createDrbdVlmData()
    {
        return new DrbdVlmDataStlt();
    }

    public CryptSetupData createCryptSetupData(String identifier, byte[] password)
    {
        return new CryptSetupStltData(
            password,
            identifier
        );
    }

}
