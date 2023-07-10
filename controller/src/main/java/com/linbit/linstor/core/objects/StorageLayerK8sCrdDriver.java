package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.StorageLayerSQLDbDriver.StorVlmInfoData;
import com.linbit.linstor.core.objects.db.utils.K8sCrdUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

// TODO: rework this to use the AbsDatabaseDriver
// that also means to split this driver into single-table drivers
// also try to merge this class with StorageLayerGenericDbDriver
@Singleton
public class StorageLayerK8sCrdDriver implements StorageLayerCtrlDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final LayerResourceIdDatabaseDriver rscIdDriver;
    private final TransactionObjectFactory transObjFactory;
    private final AccessContext dbCtx;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> storPoolDriver;

    private HashMap<Integer, HashMap<Integer, StorVlmInfoData>> cachedStorVlmInfoByRscLayerId;

    @Inject
    public StorageLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef,
        LayerResourceIdDatabaseDriver rscIdDriverRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
        rscIdDriver = rscIdDriverRef;

        storPoolDriver = (parent, ignored) -> insertOrUpdate(parent, false);
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscIdDriver;
    }

    @Override
    public void clearLoadAllCache()
    {
        cachedStorVlmInfoByRscLayerId.clear();
        cachedStorVlmInfoByRscLayerId = null;
    }

    @Override
    public void fetchForLoadAll(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef)
        throws DatabaseException
    {
        cachedStorVlmInfoByRscLayerId = new HashMap<>();

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerStorageVolumesSpec> storageVlmSpecMap = tx.getSpec(
            GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES
        );

        int rscLayerId = -1;
        try
        {
            for (GenCrdCurrent.LayerStorageVolumesSpec storVlmSpec : storageVlmSpecMap.values())
            {
                rscLayerId = storVlmSpec.layerResourceId;
                int vlmNr = storVlmSpec.vlmNr;

                HashMap<Integer, StorVlmInfoData> infoMap = cachedStorVlmInfoByRscLayerId.get(rscLayerId);
                if (infoMap == null)
                {
                    infoMap = new HashMap<>();
                    cachedStorVlmInfoByRscLayerId.put(rscLayerId, infoMap);
                }
                NodeName nodeName = new NodeName(storVlmSpec.nodeName);
                StorPoolName storPoolName = new StorPoolName(storVlmSpec.storPoolName);
                Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                    new Pair<>(nodeName, storPoolName)
                );
                infoMap.put(
                    vlmNr,
                    new StorVlmInfoData(
                        rscLayerId,
                        vlmNr,
                        DeviceProviderKind.valueOf(storVlmSpec.providerKind),
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


        int vlmNrInt = -1;
        try
        {
            Map<Integer, StorVlmInfoData> vlmInfoMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                cachedStorVlmInfoByRscLayerId,
                rscIdRef
            );
            for (Entry<Integer, StorVlmInfoData> entry : vlmInfoMap.entrySet())
            {
                vlmNrInt = entry.getKey();
                StorVlmInfoData vlmInfo = entry.getValue();

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);
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
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new LinStorDBRuntimeException(
                String.format(
                    "Failed to restore stored volume number %d for (layered) resource id: %d",
                    vlmNrInt,
                    rscIdRef
                )
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
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
            case STORAGE_SPACES:    // fall-through
            case STORAGE_SPACES_THIN:
                vlmProviderObj = new StorageSpacesData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.kind,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
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
            case REMOTE_SPDK:
                vlmProviderObj = new SpdkData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.kind,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case EXOS:
                vlmProviderObj = new ExosData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case EBS_INIT: // fall-through
            case EBS_TARGET:
                vlmProviderObj = new EbsData<>(
                    vlmRef,
                    rscDataRef,
                    vlmInfo.kind,
                    vlmInfo.storPool,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case OPENFLEX_TARGET:
                throw new ImplementationError(
                    "Openflex volumes should be loaded by openflex db driver, not by storage layer driver"
                );
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
    public void fetchForLoadAll(
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap
    )
        throws DatabaseException
    {
        // no special *Definition data to load
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
        DeviceProviderKind providerKind = vlmDataRef.getProviderKind();
        if (providerKind.equals(DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER))
        {
            throw new ImplementationError(
                "The given volume is not a storage volume, but a " + vlmDataRef.getLayerKind() +
                    "! Use appropriate database driver"
            );
        }
        insertOrUpdate(vlmDataRef, true);
    }

    private void insertOrUpdate(VlmProviderObject<?> vlmDataRef, boolean isNew)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool sp = vlmDataRef.getStorPool();
        GenCrdCurrent.LayerStorageVolumes val = GenCrdCurrent.createLayerStorageVolumes(
            vlmDataRef.getRscLayerObject().getRscLayerId(),
            vlmDataRef.getVlmNr().value,
            vlmDataRef.getProviderKind().name(),
            sp.getNode().getName().value,
            sp.getName().value
        );

        tx.createOrReplace(GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES, val, isNew);
    }

    @Override
    public void delete(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting StorageVlmData %s", getId(vlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES,
            GenCrdCurrent.createLayerStorageVolumes(
                vlmDataRef.getRscLayerObject().getRscLayerId(),
                vlmDataRef.getVlmNr().value,
                null,
                null,
                null
            )
        );
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver()
    {
        return storPoolDriver;
    }
}
