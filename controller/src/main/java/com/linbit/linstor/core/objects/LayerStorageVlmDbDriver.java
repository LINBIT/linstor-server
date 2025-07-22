package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerStorageVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.diskless.DisklessData;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.function.Function;

@Singleton
public class LayerStorageVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, StorageRscData<?>, VlmProviderObject<?>>
    implements LayerStorageVlmDatabaseDriver
{
    private final AccessContext dbCtx;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;
    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> storPoolDriver;

    @Inject
    public LayerStorageVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_STORAGE_VOLUMES,
            dbEngineRef
        );
        dbCtx = dbCtxRef;
        rscLayerIdDriver = rscLayerIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(LayerStorageVolumes.LAYER_RESOURCE_ID, vlmData -> vlmData.getRscLayerObject().getRscLayerId());
        setColumnSetter(LayerStorageVolumes.VLM_NR, vlmData -> vlmData.getVlmNr().value);
        setColumnSetter(LayerStorageVolumes.PROVIDER_KIND, vlmData -> vlmData.getProviderKind().name());
        setColumnSetter(
            LayerStorageVolumes.NODE_NAME,
            vlmData -> storVlmDataToStorPool(vlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerStorageVolumes.STOR_POOL_NAME,
            vlmData -> storVlmDataToStorPool(vlmData, this::storPoolToSpNameStr)
        );

        storPoolDriver = generateMultiColumnDriver(
            generateSingleColumnDriver(
                LayerStorageVolumes.NODE_NAME,
                vlmData -> storPoolToNodeNameStr(vlmData.getStorPool()),
                this::storPoolToNodeNameStr,
                this::storPoolToNodeNameStr
            ),
            generateSingleColumnDriver(
                LayerStorageVolumes.STOR_POOL_NAME,
                vlmData -> storPoolToSpNameStr(vlmData.getStorPool()),
                this::storPoolToSpNameStr,
                this::storPoolToSpNameStr
            )
        );
    }

    private String storVlmDataToStorPool(VlmProviderObject<?> vlmData, Function<StorPool, String> spToStrFunc)
    {
        return spToStrFunc.apply(vlmData.getStorPool());
    }

    private String storPoolToNodeNameStr(StorPool sp)
    {
        String ret = null;
        if (sp != null)
        {
            ret = sp.getNode().getName().value;
        }
        return ret;
    }

    private String storPoolToSpNameStr(StorPool sp)
    {
        String ret = null;
        if (sp != null)
        {
            ret = sp.getName().value;
        }
        return ret;
    }
    @SuppressWarnings("unchecked")
    @Override
    protected Pair<VlmProviderObject<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, StorageRscData<?>, VlmProviderObject<?>> parentRef
    )
        throws ValueOutOfRangeException, InvalidNameException, DatabaseException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerStorageVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerStorageVolumes.VLM_NR, VolumeNumber::new);
        NodeName storPoolNodeName = rawRef.build(LayerStorageVolumes.NODE_NAME, NodeName::new);
        StorPoolName storPoolName = rawRef.build(LayerStorageVolumes.STOR_POOL_NAME, StorPoolName::new);
        DeviceProviderKind providerKind = rawRef.build(LayerStorageVolumes.PROVIDER_KIND, DeviceProviderKind.class);

        StorageRscData<?> storageRscData = parentRef.getRscData(lri);
        AbsResource<?> absResource = storageRscData.getAbsResource();
        AbsVolume<?> absVlm = absResource.getVolume(vlmNr);

        PairNonNull<StorPool, InitMaps> pair = parentRef.storPoolWithInitMap.get(
            new PairNonNull<>(
                storPoolNodeName,
                storPoolName
            )
        );
        StorPool storPool = pair.objA;
        StorPool.InitMaps storPoolInitMap = pair.objB;

        AbsStorageVlmData<?> vlmProviderObj;
        if (absResource instanceof Resource)
        {
            vlmProviderObj = this.<Resource>createAbsStorageVlmData(
                (Volume) absVlm,
                (StorageRscData<Resource>) storageRscData,
                storPool,
                providerKind,
                storPoolInitMap.getVolumeMap()
            );
        }
        else
        {
            vlmProviderObj = this.<Snapshot>createAbsStorageVlmData(
                (SnapshotVolume) absVlm,
                (StorageRscData<Snapshot>) storageRscData,
                storPool,
                providerKind,
                storPoolInitMap.getSnapshotVolumeMap()
            );
        }

        return new Pair<>(vlmProviderObj, null);
    }

    private <RSC extends AbsResource<RSC>> AbsStorageVlmData<RSC> createAbsStorageVlmData(
        AbsVolume<RSC> absVlmRef,
        StorageRscData<RSC> storageRscDataRef,
        StorPool storPoolRef,
        DeviceProviderKind providerKindRef,
        Map<String, VlmProviderObject<RSC>> storPoolInitMapRef
    )
        throws DatabaseException, AccessDeniedException
    {
        AbsStorageVlmData<RSC> vlmProviderObj;
        switch (providerKindRef)
        {
            case DISKLESS:
                // no special database table for diskless DRBD.
                vlmProviderObj = new DisklessData<>(
                    absVlmRef,
                    storageRscDataRef,
                    absVlmRef.getVolumeSize(dbCtx),
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case LVM:
                vlmProviderObj = new LvmData<>(
                    absVlmRef,
                    storageRscDataRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case LVM_THIN:
                vlmProviderObj = new LvmThinData<>(
                    absVlmRef,
                    storageRscDataRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case STORAGE_SPACES: // fall-through
            case STORAGE_SPACES_THIN:
                vlmProviderObj = new StorageSpacesData<>(
                    absVlmRef,
                    storageRscDataRef,
                    providerKindRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case ZFS: // fall-trough
            case ZFS_THIN:
                vlmProviderObj = new ZfsData<>(
                    absVlmRef,
                    storageRscDataRef,
                    providerKindRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case FILE: // fall-through
            case FILE_THIN:
                vlmProviderObj = new FileData<>(
                    absVlmRef,
                    storageRscDataRef,
                    providerKindRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case SPDK:
            case REMOTE_SPDK:
                vlmProviderObj = new SpdkData<>(
                    absVlmRef,
                    storageRscDataRef,
                    providerKindRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case EBS_INIT: // fall-through
            case EBS_TARGET:
                vlmProviderObj = new EbsData<>(
                    absVlmRef,
                    storageRscDataRef,
                    providerKindRef,
                    storPoolRef,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unhandled storage type: " + providerKindRef);
        }

        storPoolInitMapRef.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);

        return vlmProviderObj;
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver()
    {
        return storPoolDriver;
    }
}
