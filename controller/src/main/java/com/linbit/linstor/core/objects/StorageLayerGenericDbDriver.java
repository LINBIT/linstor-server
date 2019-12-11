package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
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
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PROVIDER_KIND;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.STOR_POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_STORAGE_VOLUMES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("checkstyle:magicnumber")
@Singleton
public class StorageLayerGenericDbDriver implements StorageLayerCtrlDatabaseDriver
{
    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        PROVIDER_KIND,
        NODE_NAME,
        STOR_POOL_NAME
    };

    private static final String SELECT_ALL_STOR_VLMS =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_STORAGE_VOLUMES;

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_STORAGE_VOLUMES +
        " ( " + StringUtils.join(", ", VLM_ALL_FIELDS) + " )" +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) + ")";

    private static final String UPDATE_STOR_POOL =
        " UPDATE " + TBL_LAYER_STORAGE_VOLUMES +
            " SET "   + STOR_POOL_NAME + " = ? " +
            " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                        VLM_NR            + " = ?";

    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_STORAGE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ?";

    private final ErrorReporter errorReporter;
    private final AccessContext dbCtx;
    private final ResourceLayerIdDatabaseDriver rscIdDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> storPoolDriver;

    private Map<Integer, List<StorVlmInfoData>> cachedStorVlmInfoByRscLayerId;

    @Inject
    public StorageLayerGenericDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext accCtx,
        ResourceLayerIdDatabaseDriver rscIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        dbCtx = accCtx;
        rscIdDriver = rscIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        storPoolDriver = new StorPoolDriver();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return rscIdDriver;
    }

    @Override
    public void fetchForLoadAll(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef)
        throws DatabaseException
    {
        loadStorVlmsIntoCache(tmpStorPoolMapRef);
        // will be extended later with loadStorSnapVlmsIntoCache(tmpStorPoolMapRef);
    }

    private void loadStorVlmsIntoCache(
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        cachedStorVlmInfoByRscLayerId = new HashMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_VLMS))
        {
            int rscLayerId = -1;
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    rscLayerId = resultSet.getInt(LAYER_RESOURCE_ID);
                    List<StorVlmInfoData> infoList = cachedStorVlmInfoByRscLayerId.get(rscLayerId);
                    if (infoList == null)
                    {
                        infoList = new ArrayList<>();
                        cachedStorVlmInfoByRscLayerId.put(rscLayerId, infoList);
                    }
                    NodeName nodeName = new NodeName(resultSet.getString(NODE_NAME));
                    StorPoolName storPoolName = new StorPoolName(resultSet.getString(STOR_POOL_NAME));
                    Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                        new Pair<>(nodeName, storPoolName)
                    );
                    infoList.add(
                        new StorVlmInfoData(
                            rscLayerId,
                            resultSet.getInt(VLM_NR),
                            LinstorParsingUtils.asProviderKind(resultSet.getString(PROVIDER_KIND)),
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void clearLoadAllCache()
    {
        cachedStorVlmInfoByRscLayerId.clear();
        cachedStorVlmInfoByRscLayerId = null;
    }

    @Override
    public void loadLayerData(
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap
    ) throws DatabaseException
    {
        // no-op - no provider needs special resource- or volume-definition prefetching
    }

    @Override
    public <RSC extends AbsResource<RSC>> Pair<StorageRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC resourceRef,
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
            resourceRef,
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
                    AbsVolume<RSC> vlm = resourceRef.getVolume(vlmNr);

                    if (vlm == null)
                    {
                        throw new LinStorRuntimeException(
                            "Storage volume found but linstor volume missing: " +
                            resourceRef + ", vlmNr: " + vlmNr
                        );
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
                    vlmRef.getVolumeSize(dbCtx),
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
                vlmProviderObj = new SpdkData<>(
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
    public void persist(StorageRscData<?> storageRscDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void delete(StorageRscData<?> storgeRscDataRef)
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void persist(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating StorageVolume %s", getId(vlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, vlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, vlmDataRef.getVlmNr().value);
            stmt.setString(3, vlmDataRef.getProviderKind().name());
            stmt.setString(4, vlmDataRef.getStorPool().getNode().getName().value);
            stmt.setString(5, vlmDataRef.getStorPool().getName().value);

            stmt.executeUpdate();
            errorReporter.logTrace("StorageVolume created %s", getId(vlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(VlmProviderObject<?> vlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting StorageVolume %s", getId(vlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, vlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, vlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("StorageVolume deleted %s", getId(vlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool> getStorPoolDriver()
    {
        return storPoolDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(VlmProviderObject<?> vlmData)
    {
        return vlmData.getProviderKind().name() +
            "( rscId: " + vlmData.getRscLayerObject().getRscLayerId() +
            ", vlmNr:" + vlmData.getVlmNr() + ")";
    }

    private class StorPoolDriver implements SingleColumnDatabaseDriver<VlmProviderObject<?>, StorPool>
    {
        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void update(VlmProviderObject<?> parent, StorPool storPool) throws DatabaseException
        {
            errorReporter.logTrace("Updating VlmProviderObject's StorPool from [%s] to [%s] %s",
                parent.getStorPool().getName().displayValue,
                storPool.getName().displayValue,
                getId(parent)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_STOR_POOL))
            {
                stmt.setString(1, storPool.getName().value);
                stmt.setInt(2, parent.getRscLayerObject().getRscLayerId());
                stmt.setInt(3, parent.getVlmNr().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace("VlmProviderObject's StorPool updated from [%s] to [%s] %s",
                parent.getStorPool().getName().displayValue,
                storPool.getName().displayValue,
                getId(parent)
            );
        }
    }

    public static class StorVlmInfoData
    {
        public final int rscId;
        public final int vlmNr;
        public final DeviceProviderKind kind;
        public final StorPool storPool;
        public final StorPool.InitMaps storPoolInitMaps;

        public StorVlmInfoData(
            int rscIdRef,
            int vlmNrRef,
            DeviceProviderKind kindRef,
            StorPool storPoolRef,
            StorPool.InitMaps storPoolInitMapsRef
        )
        {
            rscId = rscIdRef;
            vlmNr = vlmNrRef;
            kind = kindRef;
            storPool = storPoolRef;
            storPoolInitMaps = storPoolInitMapsRef;
        }
    }
}
