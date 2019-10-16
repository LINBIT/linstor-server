package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.StorageLayerGenericDbDriver.StorVlmInfoData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerStorageVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// TODO: rework this to use the AbsDatabaseDriver
// that also means to split this driver into single-table drivers
// also try to merge this class with StorageLayerGenericDbDriver
@Singleton
public class StorageLayerETCDDriver extends BaseEtcdDriver implements StorageLayerCtrlDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver rscIdDriver;
    private final TransactionObjectFactory transObjFactory;
    private final AccessContext dbCtx;
    private final SwordfishETCDDriver sfDbDriver;

    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> storPoolDriver;

    private Map<Integer, List<StorVlmInfoData>> cachedStorVlmInfoByRscLayerId;

    @Inject
    public StorageLayerETCDDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef,
        ResourceLayerIdDatabaseDriver rscIdDriverRef,
        SwordfishETCDDriver sfDbDriverRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
        rscIdDriver = rscIdDriverRef;
        sfDbDriver = sfDbDriverRef;

        storPoolDriver = (parent, storPool) ->
        {
            namespace(
                GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES,
                Integer.toString(parent.getRscLayerObject().getRscLayerId()),
                Integer.toString(parent.getVlmNr().value)
            )
                .put(LayerStorageVolumes.STOR_POOL_NAME, parent.getStorPool().getName().value);
        };
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return rscIdDriver;
    }

    @Override
    public void clearLoadAllCache()
    {
        cachedStorVlmInfoByRscLayerId.clear();
        cachedStorVlmInfoByRscLayerId = null;

        sfDbDriver.clearLoadAllCache();
    }

    @Override
    public void fetchForLoadAll(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef)
        throws DatabaseException
    {
        cachedStorVlmInfoByRscLayerId = new HashMap<>();

        Map<String, String> allVlmDataMap = namespace(GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES)
            .get(true);
        Set<String> composedPkSet = EtcdUtils.getComposedPkList(allVlmDataMap);
        int rscLayerId = -1;
        try
        {
            for (String composedPk : composedPkSet)
            {
                String[] pks = composedPk.split(EtcdUtils.PK_DELIMITER);

                rscLayerId = Integer.parseInt(pks[LayerStorageVolumes.LAYER_RESOURCE_ID.getIndex()]);
                int vlmNr = Integer.parseInt(pks[LayerStorageVolumes.VLM_NR.getIndex()]);

                List<StorVlmInfoData> infoList = cachedStorVlmInfoByRscLayerId.get(rscLayerId);
                if (infoList == null)
                {
                    infoList = new ArrayList<>();
                    cachedStorVlmInfoByRscLayerId.put(rscLayerId, infoList);
                }
                NodeName nodeName = new NodeName(
                    allVlmDataMap.get(
                        EtcdUtils.buildKey(LayerStorageVolumes.NODE_NAME, pks)
                    )
                );
                StorPoolName storPoolName = new StorPoolName(
                    allVlmDataMap.get(
                        EtcdUtils.buildKey(LayerStorageVolumes.STOR_POOL_NAME, pks)
                    )
                );
                Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                    new Pair<>(nodeName, storPoolName)
                );
                infoList.add(
                    new StorVlmInfoData(
                        rscLayerId,
                        vlmNr,
                        DeviceProviderKind.valueOf(
                            allVlmDataMap.get(
                                EtcdUtils.buildKey(LayerStorageVolumes.PROVIDER_KIND, pks)
                            )
                        ),
                        storPoolWithInitMap.objA,
                        storPoolWithInitMap.objB
                    )
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                String.format(
                    "Failed to restore stored name '%s' of (layered) resource id: %d",
                    exc.invalidName,
                    rscLayerId
                )
            );
        }
    }

    @Override
    public <RSC extends AbsResource<RSC>> Pair<StorageRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int rscIdRef,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws AccessDeniedException, DatabaseException
    {
        Map<VolumeNumber, VlmProviderObject<RSC>> vlmMap = new TreeMap<>();
        StorageRscData<RSC> storageRscData = new StorageRscData<>(
            rscIdRef,
            parentRef,
            absRsc,
            rscSuffixRef,
            vlmMap,
            this,
            transObjFactory,
            transMgrProvider
        );

        List<StorVlmInfoData> vlmInfoList = cachedStorVlmInfoByRscLayerId.get(rscIdRef);

        if (vlmInfoList != null)
        {
            for (StorVlmInfoData vlmInfo : vlmInfoList)
            {
                try
                {
                    VolumeNumber vlmNr = new VolumeNumber(vlmInfo.vlmNr);
                    AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);

                    if (vlm == null)
                    {
                        if (absRsc instanceof Resource)
                        {
                            throw new LinStorRuntimeException(
                                "Storage volume found but linstor volume missing: " +
                                    absRsc + ", vlmNr: " + vlmNr
                            );
                        }
                        else
                        {
                            throw new LinStorRuntimeException(
                                "Storage snapshot volume found but linstor snapshot volume missing: " +
                                    absRsc + ", vlmNr: " + vlmNr
                            );
                        }
                    }

                    VlmProviderObject<RSC> vlmData = loadVlmProviderObject(vlm, storageRscData, vlmInfo);
                    vlmMap.put(vlmNr, vlmData);
                }
                catch (ValueOutOfRangeException exc)
                {
                    throw new LinStorDBRuntimeException(
                        String.format(
                            "Failed to restore stored volume number %d for (layered) resource id: %d",
                            vlmInfo.vlmNr,
                            vlmInfo.rscId
                        )
                    );
                }
            }
        }

        return new Pair<>(
            storageRscData,
            null // storage resources have no children
        );
    }

    private <RSC extends AbsResource<RSC>> VlmProviderObject<RSC> loadVlmProviderObject(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        StorVlmInfoData vlmInfo
    )
        throws AccessDeniedException, DatabaseException
    {
        VlmProviderObject<RSC> vlmProviderObj;
        switch (vlmInfo.kind)
        {
            case DISKLESS:
                // no special database table for diskless DRBD.
                vlmProviderObj = new DisklessData<>(
                    vlmRef,
                    rscDataRef,
                    vlmRef.getVolumeDefinition().getVolumeSize(dbCtx),
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case LVM:
                vlmProviderObj = new LvmData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case LVM_THIN:
                vlmProviderObj = new LvmThinData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case SWORDFISH_INITIATOR: // fall-through
            case SWORDFISH_TARGET:
                vlmProviderObj = sfDbDriver.load(vlmRef, rscDataRef, vlmInfo.kind, vlmInfo.storPool, this);
                break;
            case ZFS: // fall-trough
            case ZFS_THIN:
                vlmProviderObj = new ZfsData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.kind,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case FILE: // fall-through
            case FILE_THIN:
                vlmProviderObj = new FileData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.kind,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case SPDK:
                vlmProviderObj = new SpdkData(
                        vlmRef,
                        rscDataRef,
                        vlmInfo.storPool,
                        this,
                        transObjFactory,
                        transMgrProvider
                );
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unhandled storage type: " + vlmInfo.kind);
        }
        if (vlmRef instanceof Volume)
        {
            vlmInfo.storPoolInitMaps.getVolumeMap().put(
                vlmProviderObj.getVolumeKey(),
                (VlmProviderObject<Resource>) vlmProviderObj
            );
        }
        else
        {
            vlmInfo.storPoolInitMaps.getSnapshotVolumeMap().put(
                vlmProviderObj.getVolumeKey(),
                (VlmProviderObject<Snapshot>) vlmProviderObj
            );
        }
        return vlmProviderObj;
    }

    @Override
    public void loadLayerData(
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap
    )
        throws DatabaseException
    {
        sfDbDriver.loadLayerData(rscDfnMap);
    }

    @Override
    public void persist(StorageRscData<?> storageRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void delete(StorageRscData<?> storgeRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void persist(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating StorageVolume %s", getId(vlmDataRef));
        namespace(
            GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES,
            Integer.toString(vlmDataRef.getRscLayerObject().getRscLayerId()),
            Integer.toString(vlmDataRef.getVlmNr().value)
        )
            .put(LayerStorageVolumes.PROVIDER_KIND, vlmDataRef.getProviderKind().name())
            .put(LayerStorageVolumes.NODE_NAME, vlmDataRef.getStorPool().getNode().getName().value)
            .put(LayerStorageVolumes.STOR_POOL_NAME, vlmDataRef.getStorPool().getName().value);
    }

    @Override
    public void delete(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        /*
         * DO NOT USE ranged delete!
         * same issue as described in NodeETCDDriver, but with toggle disk (remove diskless
         * vlmData, insert diskfull vlmData in same txn)
         */
        String[] pk = new String[] {
            Integer.toString(vlmDataRef.getRscLayerObject().getRscLayerId()),
            Integer.toString(vlmDataRef.getVlmNr().value)
        };
        for (Column col : LayerStorageVolumes.ALL)
        {
            if (!col.isPk())
            {
                namespace(EtcdUtils.buildKey(col, pk)).delete(false);
            }
        }
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver()
    {
        return storPoolDriver;
    }

    public static String getId(VlmProviderObject<?> vlmData)
    {
        return vlmData.getProviderKind().name() +
            "( rscId: " + vlmData.getRscLayerObject().getRscLayerId() +
            ", vlmNr:" + vlmData.getVlmNr() + ")";
    }

}
