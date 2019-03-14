package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.drdbdiskless.DrbdDisklessData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PROVIDER_KIND;
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
public class StorageLayerGenericDbDriver implements StorageLayerDatabaseDriver
{
    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        PROVIDER_KIND
    };

    private static final String SELECT_ALL_STOR_VLMS =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_STORAGE_VOLUMES;

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_STORAGE_VOLUMES +
        " ( " + StringUtils.join(", ", VLM_ALL_FIELDS) + " )" +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) + ")";

    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_STORAGE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ?";

    private final ErrorReporter errorReporter;
    private final AccessContext dbCtx;
    private final ResourceLayerIdDatabaseDriver rscIdDriver;
    private final SwordfishLayerGenericDbDriver sfDbDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    private Map<Integer, List<StorVlmInfoData>> cachedStorVlmInfoByRscLayerId;

    @Inject
    public StorageLayerGenericDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext accCtx,
        ResourceLayerIdDatabaseDriver rscIdDriverRef,
        SwordfishLayerGenericDbDriver sfDbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        dbCtx = accCtx;
        rscIdDriver = rscIdDriverRef;
        sfDbDriver = sfDbDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return rscIdDriver;
    }

    public void fetchForLoadAll() throws SQLException
    {
        cachedStorVlmInfoByRscLayerId = new HashMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_STOR_VLMS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    int rscLayerId = resultSet.getInt(LAYER_RESOURCE_ID);
                    List<StorVlmInfoData> infoList = cachedStorVlmInfoByRscLayerId.get(rscLayerId);
                    if (infoList == null)
                    {
                        infoList = new ArrayList<>();
                        cachedStorVlmInfoByRscLayerId.put(rscLayerId, infoList);
                    }
                    infoList.add(
                        new StorVlmInfoData(
                            rscLayerId,
                            resultSet.getInt(VLM_NR),
                            LinstorParsingUtils.asProviderKind(resultSet.getString(PROVIDER_KIND))
                        )
                    );
                }
            }
        }
    }

    public void clearLoadAllCache()
    {
        cachedStorVlmInfoByRscLayerId.clear();
        cachedStorVlmInfoByRscLayerId = null;
    }

    public Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource resourceRef,
        int rscIdRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws AccessDeniedException
    {
        Map<VolumeNumber, VlmProviderObject> vlmMap = new TreeMap<>();
        StorageRscData storageRscData = new StorageRscData(
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
        for (StorVlmInfoData vlmInfo : vlmInfoList)
        {
            try
            {
                VolumeNumber vlmNr = new VolumeNumber(vlmInfo.vlmNr);
                Volume vlm = resourceRef.getVolume(vlmNr);

                if (vlm == null)
                {
                    throw new LinStorRuntimeException(
                        "Storage volume found but linstor volume missing: " +
                        resourceRef + ", vlmNr: " + vlmNr
                    );
                }

                VlmProviderObject vlmData = loadVlmProviderObject(vlm, storageRscData, vlmInfo);
                vlmMap.put(vlmNr, vlmData);
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorSqlRuntimeException(
                    String.format(
                        "Failed to restore stored volume number %d for (layered) resource id: %d",
                        vlmInfo.vlmNr,
                        vlmInfo.rscId
                    )
                );
            }
        }

        return new Pair<>(
            storageRscData,
            null // storage resources have no children
        );
    }

    private VlmProviderObject loadVlmProviderObject(
        Volume vlmRef,
        StorageRscData rscDataRef,
        StorVlmInfoData vlmInfo
    )
        throws AccessDeniedException
    {
        VlmProviderObject vlmProviderObj;
        switch (vlmInfo.kind)
        {
            case DRBD_DISKLESS:
                // no special database table for diskless DRBD.
                vlmProviderObj = new DrbdDisklessData(
                    vlmRef,
                    rscDataRef,
                    vlmRef.getVolumeDefinition().getVolumeSize(dbCtx),
                    transObjFactory,
                    transMgrProvider
                );
                break;
            case LVM:
                vlmProviderObj = new LvmData(vlmRef, rscDataRef, transObjFactory, transMgrProvider);
                break;
            case LVM_THIN:
                vlmProviderObj = new LvmThinData(vlmRef, rscDataRef, transObjFactory, transMgrProvider);
                break;
            case SWORDFISH_INITIATOR: // fall-through
            case SWORDFISH_TARGET:
                vlmProviderObj = sfDbDriver.load(vlmRef, rscDataRef, vlmInfo.kind);
                break;
            case ZFS: // fall-trough
            case ZFS_THIN:
                vlmProviderObj = new ZfsData(vlmRef, rscDataRef, vlmInfo.kind, transObjFactory, transMgrProvider);
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unhandled storage type: " + vlmInfo.kind);
        }
        return vlmProviderObj;
    }

    @Override
    public void persist(StorageRscData storageRscDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void delete(StorageRscData storgeRscDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if StorageRscData will get a database table in future.
    }

    @Override
    public void persist(VlmProviderObject vlmDataRef) throws SQLException
    {
        errorReporter.logTrace("Creating StorageVolume %s", getId(vlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, vlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, vlmDataRef.getVlmNr().value);
            stmt.setString(3, vlmDataRef.getProviderKind().name());

            stmt.executeUpdate();
            errorReporter.logTrace("StorageVolume created %s", getId(vlmDataRef));
        }
    }

    @Override
    public void delete(VlmProviderObject vlmDataRef) throws SQLException
    {
        errorReporter.logTrace("Deleting StorageVolume %s", getId(vlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, vlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, vlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("StorageVolume deleted %s", getId(vlmDataRef));
        }
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(VlmProviderObject vlmData)
    {
        return vlmData.getProviderKind().name() +
            "( rscId: " + vlmData.getRscLayerObject().getRscLayerId() +
            ", vlmNr:" + vlmData.getVlmNr() + ")";
    }

    public static class StorVlmInfoData
    {
        public final int rscId;
        public final int vlmNr;
        public final DeviceProviderKind kind;

        public StorVlmInfoData(int rscIdRef, int vlmNrRef, DeviceProviderKind kindRef)
        {
            super();
            rscId = rscIdRef;
            vlmNr = vlmNrRef;
            kind = kindRef;
        }
    }
}
