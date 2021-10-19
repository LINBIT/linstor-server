package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.AL_STRIPES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.AL_STRIPE_SIZE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.FLAGS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PEER_SLOTS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.SECRET;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_RESOURCES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_RESOURCE_DEFINITIONS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_VOLUMES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_VOLUME_DEFINITIONS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TCP_PORT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TRANSPORT_TYPE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_MINOR_NR;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("checkstyle:magicnumber")
@Singleton
public class DrbdLayerSQLDbDriver implements DrbdLayerCtrlDatabaseDriver
{
    private static final String[] RSC_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        PEER_SLOTS,
        AL_STRIPES,
        AL_STRIPE_SIZE,
        FLAGS,
        NODE_ID
    };
    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        NODE_NAME,
        POOL_NAME
    };
    private static final String[] RSC_DFN_ALL_FIELDS =
    {
        RESOURCE_NAME,
        RESOURCE_NAME_SUFFIX,
        SNAPSHOT_NAME,
        PEER_SLOTS,
        AL_STRIPES,
        AL_STRIPE_SIZE,
        TCP_PORT,
        TRANSPORT_TYPE,
        SECRET
    };
    private static final String[] VLM_DFN_ALL_FIELDS =
    {
        RESOURCE_NAME,
        RESOURCE_NAME_SUFFIX,
        SNAPSHOT_NAME,
        VLM_NR,
        VLM_MINOR_NR
    };

    private static final String SELECT_RSC_BY_ID =
        " SELECT " + StringUtils.join(", ", RSC_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_RESOURCES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String SELECT_VLM_BY_RSC_ID =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String SELECT_ALL_RSC_DFN_AND_VLM_DFN =
        " SELECT " +
            joinAs(", ", "RD.", "RD_", RSC_DFN_ALL_FIELDS) + ", " +
            joinAs(", ", "VD.", "VD_", VLM_DFN_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS + " AS RD" +
        " LEFT OUTER JOIN " + TBL_LAYER_DRBD_VOLUME_DEFINITIONS + " AS VD" +
            " ON RD." + RESOURCE_NAME + " = VD." + RESOURCE_NAME + " AND" +
            "    RD." + RESOURCE_NAME_SUFFIX + " = VD." + RESOURCE_NAME_SUFFIX + " AND " +
            "    RD." + SNAPSHOT_NAME + " = VD." + SNAPSHOT_NAME;

    private static final String INSERT_RSC =
        " INSERT INTO " + TBL_LAYER_DRBD_RESOURCES +
        " ( " + StringUtils.join(", ", RSC_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", RSC_ALL_FIELDS.length) + " )";
    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_DRBD_VOLUMES +
        " ( " + StringUtils.join(", ", VLM_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) + " )";
    private static final String INSERT_RSC_DFN =
        " INSERT INTO " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " ( " + StringUtils.join(", ", RSC_DFN_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", RSC_DFN_ALL_FIELDS.length) + " )";
    private static final String INSERT_VLM_DFN =
        " INSERT INTO " + TBL_LAYER_DRBD_VOLUME_DEFINITIONS +
        " ( " + StringUtils.join(", ", VLM_DFN_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_DFN_ALL_FIELDS.length) + " )";

    private static final String DELETE_RSC =
        " DELETE FROM " + TBL_LAYER_DRBD_RESOURCES +
        " WHERE " + LAYER_RESOURCE_ID    + " = ?";
    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_DRBD_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR +            " = ?";
    private static final String DELETE_RSC_DFN =
        " DELETE FROM " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ?";
    private static final String DELETE_VLM_DFN =
        " DELETE FROM " + TBL_LAYER_DRBD_VOLUME_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ? AND " +
                    VLM_NR               + " = ?";

    private static final String UPDATE_RSC_FLAGS =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCES +
        " SET " + FLAGS + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String UPDATE_RSC_NODE_ID =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCES +
        " SET " + NODE_ID + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String UPDATE_VLM_EXT_STOR_POOL =
        " UPDATE " + TBL_LAYER_DRBD_VOLUMES +
        " SET " + NODE_NAME + " = ?, " +
                  POOL_NAME + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ?";
    private static final String UPDATE_RSC_DFN_SECRET =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + SECRET + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ?";
    private static final String UPDATE_RSC_DFN_TCP_PORT =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + TCP_PORT + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ?";
    private static final String UPDATE_RSC_DFN_TRANSPORT_TYPE =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + TRANSPORT_TYPE + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ?";
    private static final String UPDATE_RSC_DFN_PEER_SLOTS =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + PEER_SLOTS + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    SNAPSHOT_NAME        + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final RscFlagsDriver rscStatePersistence;
    private final RscNodeIdDriver rscNodeIdDriver;
    private final VlmExtStorPoolDriver vlmExtStorPoolDriver;
    private final RscDfnSecretDriver rscDfnSecretDriver;
    private final RscDfnTcpPortDriver rscDfnTcpPortDriver;
    private final RscDfnTransportTypeDriver rscDfnTransportTypeDriver;
    private final RscDfnPeerSlotsDriver rscDfnPeerSlotsDriver;

    private final Map<Pair<ResourceDefinition, String>, Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>>> drbdRscDfnCache;
    private final Map<Pair<VolumeDefinition, String>, DrbdVlmDfnData<Resource>> drbdVlmDfnCache;
    private final Map<Pair<SnapshotDefinition, String>, Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>>> drbdSnapDfnCache;
    private final Map<Pair<SnapshotVolumeDefinition, String>, DrbdVlmDfnData<Snapshot>> drbdSnapVlmDfnCache;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    @Inject
    public DrbdLayerSQLDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        tcpPortPool = tcpPortPoolRef;
        minorPool = minorPoolRef;

        rscStatePersistence = new RscFlagsDriver();
        rscNodeIdDriver = new RscNodeIdDriver();
        vlmExtStorPoolDriver = new VlmExtStorPoolDriver();
        rscDfnSecretDriver = new RscDfnSecretDriver();
        rscDfnTcpPortDriver = new RscDfnTcpPortDriver();
        rscDfnTransportTypeDriver = new RscDfnTransportTypeDriver();
        rscDfnPeerSlotsDriver = new RscDfnPeerSlotsDriver();

        drbdRscDfnCache = new HashMap<>();
        drbdVlmDfnCache = new HashMap<>();
        drbdSnapDfnCache = new HashMap<>();
        drbdSnapVlmDfnCache = new HashMap<>();
    }

    private static String joinAs(String delimiter, String tablePrefix, String colPrefix, String[] array)
    {
        StringBuilder sb = new StringBuilder();
        for (String str : array)
        {
            sb.append(tablePrefix).append(str)
                .append(" AS ")
                .append(colPrefix).append(str)
                .append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    /*
     * These caches are only used during loading. Having them cleared after having loaded all data
     * these caches should never be used again.
     */
    @Override
    public void clearLoadAllCache()
    {
        drbdRscDfnCache.clear();
        drbdVlmDfnCache.clear();
    }


    /**
     * Loads the layer data for the given {@link ResourceDefinition}s if available.
     *
     * @param rscDfnMapRef
     * @throws DatabaseException
     */
    @Override
    public void fetchForLoadAll(
        Map<ResourceName, ResourceDefinition> rscDfnMapRef,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMapRef
    )
        throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RSC_DFN_AND_VLM_DFN))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ResourceName rscName = getResourceName(resultSet, "RD_" + RESOURCE_NAME);
                    String rscNameSuffix = resultSet.getString("RD_" + RESOURCE_NAME_SUFFIX);
                    String snapNameStr = resultSet.getString("RD_" + SNAPSHOT_NAME);

                    boolean restoreAsResourceDefinition = snapNameStr == null || snapNameStr.trim().isEmpty();

                    ResourceDefinition rscDfn;
                    SnapshotDefinition snapDfn;
                    DrbdRscDfnData<?> drbdRscDfnDataLoaded;

                    if (restoreAsResourceDefinition)
                    {
                        rscDfn = rscDfnMapRef.get(rscName);
                        snapDfn = null;
                        if (rscDfn == null)
                        {
                            throw new LinStorDBRuntimeException(
                                "Loaded drbd resource definition data for non existent resource definition '" +
                                    rscName + "'"
                            );
                        }
                        drbdRscDfnDataLoaded = rscDfn.getLayerData(dbCtx, DeviceLayerKind.DRBD, rscNameSuffix);
                    }
                    else
                    {
                        rscDfn = null;
                        snapDfn = snapDfnMapRef.get(new Pair<>(rscName, getSnapshotName(snapNameStr)));
                        if (snapDfn == null)
                        {
                            throw new LinStorDBRuntimeException(
                                "Loaded drbd snapshot definition data for non existent snapshot definition '" +
                                    rscName + "', '" + snapNameStr + "'"
                            );
                        }
                        drbdRscDfnDataLoaded = snapDfn.getLayerData(dbCtx, DeviceLayerKind.DRBD, rscNameSuffix);
                    }

                    if (drbdRscDfnDataLoaded == null)
                    {
                        short peerSlots = resultSet.getShort("RD_" + PEER_SLOTS);
                        int alStripes = resultSet.getInt("RD_" + AL_STRIPES);
                        long alStripeSize = resultSet.getLong("RD_" + AL_STRIPE_SIZE);
                        TransportType transportType = TransportType.byValue(
                            resultSet.getString("RD_" + TRANSPORT_TYPE)
                        );
                        Integer tcpPort;
                        String secret;
                        if (restoreAsResourceDefinition)
                        {
                            tcpPort = resultSet.getInt("RD_" + TCP_PORT);
                            secret = resultSet.getString("RD_" + SECRET);
                        }
                        else
                        {
                            tcpPort = null;
                            secret = null;
                        }

                        if (restoreAsResourceDefinition)
                        {
                            List<DrbdRscData<Resource>> rscDataList = new ArrayList<>();

                            DrbdRscDfnData<Resource> drbdRscDfnData = new DrbdRscDfnData<>(
                                rscDfn.getName(),
                                null,
                                rscNameSuffix,
                                peerSlots,
                                alStripes,
                                alStripeSize,
                                tcpPort,
                                transportType,
                                secret,
                                rscDataList,
                                new TreeMap<>(),
                                tcpPortPool,
                                this,
                                transObjFactory,
                                transMgrProvider
                            );
                            Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>> pair = new Pair<>(
                                drbdRscDfnData,
                                rscDataList
                            );
                            drbdRscDfnCache.put(new Pair<>(rscDfn, rscNameSuffix), pair);

                            rscDfn.setLayerData(dbCtx, drbdRscDfnData);
                        }
                        else
                        {
                            List<DrbdRscData<Snapshot>> rscDataList = new ArrayList<>();

                            DrbdRscDfnData<Snapshot> drbdSnapDfnData = new DrbdRscDfnData<>(
                                snapDfn.getResourceName(),
                                snapDfn.getName(),
                                rscNameSuffix,
                                peerSlots,
                                alStripes,
                                alStripeSize,
                                DrbdRscDfnData.SNAPSHOT_TCP_PORT,
                                transportType,
                                null,
                                rscDataList,
                                new TreeMap<>(),
                                tcpPortPool,
                                this,
                                transObjFactory,
                                transMgrProvider
                            );

                            Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>> pair = new Pair<>(
                                drbdSnapDfnData,
                                rscDataList
                            );
                            drbdSnapDfnCache.put(new Pair<>(snapDfn, rscNameSuffix), pair);

                            snapDfn.setLayerData(dbCtx, drbdSnapDfnData);
                        }
                    }

                    // drbdRscDfnData is now restored. If "VD_" columns are not empty, restore the DrbdVlmDfnData
                    Integer vlmNr = resultSet.getInt("VD_" + VLM_NR);
                    if (!resultSet.wasNull())
                    {
                        if (restoreAsResourceDefinition)
                        {
                            VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(dbCtx, new VolumeNumber(vlmNr));
                            if (vlmDfn == null)
                            {
                                throw new LinStorDBRuntimeException(
                                    "Loaded drbd volume definition data for non existent volume definition '" +
                                        rscName + "', vlmNr: " + vlmNr
                                );
                            }
                            Integer minor = resultSet.getInt("VD_" + VLM_MINOR_NR);

                            DrbdVlmDfnData<Resource> drbdVlmDfnData = new DrbdVlmDfnData<>(
                                vlmDfn,
                                rscDfn.getName(),
                                null,
                                rscNameSuffix,
                                vlmDfn.getVolumeNumber(),
                                minor,
                                minorPool,
                                vlmDfn.getResourceDefinition().<DrbdRscDfnData<Resource>>getLayerData(
                                    dbCtx,
                                    DeviceLayerKind.DRBD,
                                    rscNameSuffix
                                ),
                                this,
                                transMgrProvider
                            );
                            drbdVlmDfnCache.put(new Pair<>(vlmDfn, rscNameSuffix), drbdVlmDfnData);

                            vlmDfn.setLayerData(dbCtx, drbdVlmDfnData);
                        }
                        else
                        {
                            SnapshotVolumeDefinition snapVlmDfn = snapDfn.getSnapshotVolumeDefinition(
                                dbCtx,
                                new VolumeNumber(vlmNr)
                            );
                            if (snapVlmDfn == null)
                            {
                                throw new LinStorDBRuntimeException(
                                    "Loaded drbd volume definition data for non existent volume definition '" +
                                        rscName + "', vlmNr: " + vlmNr
                                );
                            }

                            DrbdVlmDfnData<Snapshot> drbdSnapVlmDfnData = new DrbdVlmDfnData<>(
                                snapVlmDfn.getVolumeDefinition(),
                                snapDfn.getResourceName(),
                                snapVlmDfn.getSnapshotName(),
                                rscNameSuffix,
                                snapVlmDfn.getVolumeNumber(),
                                DrbdVlmDfnData.SNAPSHOT_MINOR,
                                minorPool,
                                snapVlmDfn.getSnapshotDefinition().<DrbdRscDfnData<Snapshot>>getLayerData(
                                    dbCtx,
                                    DeviceLayerKind.DRBD,
                                    rscNameSuffix
                                ),
                                this,
                                transMgrProvider
                            );

                            drbdSnapVlmDfnCache.put(new Pair<>(snapVlmDfn, rscNameSuffix), drbdSnapVlmDfnData);

                            snapVlmDfn.setLayerData(dbCtx, drbdSnapVlmDfnData);
                        }
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
        catch (ValueOutOfRangeException | ExhaustedPoolException | ValueInUseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private ResourceName getResourceName(ResultSet resultSet, String columnName) throws DatabaseException
    {
        ResourceName rscName;
        try
        {
            final String rscNameStr = resultSet.getString(columnName);
            try
            {
                rscName = new ResourceName(rscNameStr);
            }
            catch (InvalidNameException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored resourceName [" + rscNameStr + "]"
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        return rscName;
    }

    private SnapshotName getSnapshotName(String snapshotNameStr)
    {
        SnapshotName snapName;
        try
        {
            snapName = new SnapshotName(snapshotNameStr);
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored resourceName [" + snapshotNameStr + "]"
            );
        }
        return snapName;
    }

    /**
     * Fully loads a {@link DrbdRscData} object, including the {@link DrbdRscDfnData}, {@link DrbdVlmData} and
     * {@link DrbdVlmDfnData}
     *
     * @param absParent
     * @param storPoolMapRef
     * @return a {@link Pair}, where the first object is the actual DrbdRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws DatabaseException
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> absParent,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolMapRef
    )
        throws DatabaseException
    {
        Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_RSC_BY_ID))
        {
            stmt.setInt(1, id);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    NodeId nodeId;
                    try
                    {
                        nodeId = new NodeId(resultSet.getInt(NODE_ID));
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Failed to restore stored nodeId [" + resultSet.getInt(NODE_ID) + "]"
                        );
                    }
                    Short peerSlots = resultSet.getShort(PEER_SLOTS);
                    if (resultSet.wasNull())
                    {
                        peerSlots = null;
                    }
                    Integer alStripes = resultSet.getInt(AL_STRIPES);
                    if (resultSet.wasNull())
                    {
                        alStripes = null;
                    }
                    Long alStripeSize = resultSet.getLong(AL_STRIPE_SIZE);
                    if (resultSet.wasNull())
                    {
                        alStripeSize = null;
                    }
                    long initFlags = resultSet.getLong(FLAGS);

                    Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
                    Object childrenAfterTypeEreasure = children; // sorry for this hack

                    if (absRsc instanceof Resource)
                    {
                        Resource rsc = (Resource) absRsc;

                        Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>> drbdRscDfnDataPair = drbdRscDfnCache
                            .get(new Pair<>(rsc.getDefinition(), rscSuffixRef));
                        Map<VolumeNumber, DrbdVlmData<Resource>> vlmMap = new TreeMap<>();

                        DrbdRscData<Resource> drbdRscData = new DrbdRscData<>(
                            id,
                            rsc,
                            (AbsRscLayerObject<Resource>) absParent,
                            drbdRscDfnDataPair.objA,
                            (Set<AbsRscLayerObject<Resource>>) childrenAfterTypeEreasure,
                            vlmMap,
                            rscSuffixRef,
                            nodeId,
                            peerSlots,
                            alStripes,
                            alStripeSize,
                            initFlags,
                            this,
                            transObjFactory,
                            transMgrProvider
                        );
                        ret = new Pair<>(
                            (DrbdRscData<RSC>) drbdRscData,
                            children
                        );
                        drbdRscDfnDataPair.objB.add(drbdRscData);

                        restoreDrbdVolumes(drbdRscData, vlmMap, storPoolMapRef);
                    }
                    else
                    {
                        Snapshot snap = (Snapshot) absRsc;
                        Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>> drbdSnapDfnDataPair =
                            drbdSnapDfnCache.get(
                                new Pair<>(snap.getSnapshotDefinition(), rscSuffixRef)
                            );

                        Map<VolumeNumber, DrbdVlmData<Snapshot>> vlmMap = new TreeMap<>();
                        DrbdRscData<Snapshot> drbdSnapData = new DrbdRscData<>(
                            id,
                            snap,
                            (AbsRscLayerObject<Snapshot>) absParent,
                            drbdSnapDfnDataPair.objA,
                            (Set<AbsRscLayerObject<Snapshot>>) childrenAfterTypeEreasure,
                            vlmMap,
                            rscSuffixRef,
                            nodeId,
                            peerSlots,
                            alStripes,
                            alStripeSize,
                            initFlags,
                            this,
                            transObjFactory,
                            transMgrProvider
                        );
                        ret = new Pair<>(
                            (DrbdRscData<RSC>) drbdSnapData,
                            children
                        );

                        drbdSnapDfnDataPair.objB.add(drbdSnapData);

                        restoreDrbdVolumes(drbdSnapData, vlmMap, storPoolMapRef);
                    }
                }
                else
                {
                    throw new ImplementationError("Requested id [" + id + "] was not found in the database");
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return ret;
    }

    private <RSC extends AbsResource<RSC>> void restoreDrbdVolumes(
        DrbdRscData<RSC> rscData,
        Map<VolumeNumber, DrbdVlmData<RSC>> vlmMap,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolMapRef
    )
        throws DatabaseException
    {
        RSC absRsc = rscData.getAbsResource();
        NodeName currentNodeName = absRsc.getNode().getName();

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_VLM_BY_RSC_ID))
        {
            stmt.setInt(1, rscData.getRscLayerId());

            int vlmNrInt = -1;
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    vlmNrInt = resultSet.getInt(VLM_NR);
                    String extMetaStorPoolNameStr = resultSet.getString(POOL_NAME);

                    VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                    StorPool extMetaDataStorPool = null;

                    if (extMetaStorPoolNameStr != null)
                    {
                        StorPoolName extStorPoolName = new StorPoolName(extMetaStorPoolNameStr);
                        extMetaDataStorPool = storPoolMapRef.get(
                            new Pair<>(currentNodeName, extStorPoolName)
                        ).objA;
                    }

                    if (absRsc instanceof Resource)
                    {
                        Volume vlm = ((Resource) absRsc).getVolume(vlmNr);

                        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                        DrbdVlmDfnData<Resource> drbdVlmDfnData = drbdVlmDfnCache.get(
                            new Pair<>(vlmDfn, rscData.getResourceNameSuffix())
                        );

                        if (drbdVlmDfnData != null)
                        {
                            vlmMap.put(
                                vlm.getVolumeDefinition().getVolumeNumber(),
                                (DrbdVlmData<RSC>) new DrbdVlmData<>(
                                    vlm,
                                    (DrbdRscData<Resource>) rscData,
                                    drbdVlmDfnData,
                                    extMetaDataStorPool,
                                    this,
                                    transObjFactory,
                                    transMgrProvider
                                )
                            );
                        }
                        else
                        {
                            throw new ImplementationError(
                                "Trying to load drbdVlm without drbdVlmDfn. " +
                                    "layerId: " + rscData.getRscLayerId() + " node name: " + currentNodeName +
                                    " resource name: " + absRsc.getResourceDefinition().getName() + " vlmNr " + vlmNrInt
                            );
                        }
                    }
                    else
                    {
                        SnapshotVolume vlm = ((Snapshot) absRsc).getVolume(vlmNr);

                        SnapshotVolumeDefinition snapVlmDfn = vlm.getSnapshotVolumeDefinition();
                        DrbdVlmDfnData<Snapshot> drbdsnapVlmDfnData = drbdSnapVlmDfnCache.get(
                            new Pair<>(snapVlmDfn, rscData.getResourceNameSuffix())
                        );

                        if (drbdsnapVlmDfnData != null)
                        {
                            vlmMap.put(
                                vlm.getVolumeNumber(),
                                (DrbdVlmData<RSC>) new DrbdVlmData<>(
                                    vlm,
                                    (DrbdRscData<Snapshot>) rscData,
                                    drbdsnapVlmDfnData,
                                    extMetaDataStorPool,
                                    this,
                                    transObjFactory,
                                    transMgrProvider
                                )
                            );
                        }
                        else
                        {
                            throw new ImplementationError(
                                "Trying to load drbdSnapVlm without drbdSnapVlmDfn. " +
                                    "layerId: " + rscData.getRscLayerId() +
                                    " node name: " + currentNodeName +
                                    " resource name: " + snapVlmDfn.getResourceName() +
                                    " snapshot name: " + snapVlmDfn.getSnapshotName() +
                                    " vlmNr " + vlmNrInt
                            );
                        }
                    }
                }
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored volume number " + vlmNrInt +
                        " for resource layer id: " + rscData.getRscLayerId()
                );
            }
            catch (InvalidNameException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored storage pool name '" + exc.invalidName +
                        "' for resource layer id " + rscData.getRscLayerId() + " vlmNr: " + vlmNrInt
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void create(DrbdRscData<?> drbdRscDataRef) throws DatabaseException
    {
        @SuppressWarnings("resource") // will be done in DbConnectionPool#returnConnection
        Connection connection = getConnection();

        errorReporter.logTrace("Creating DrbdRscData %s", getId(drbdRscDataRef));
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RSC))
        {
            stmt.setInt(1, drbdRscDataRef.getRscLayerId());
            stmt.setShort(2, drbdRscDataRef.getPeerSlots());
            stmt.setInt(3, drbdRscDataRef.getAlStripes());
            stmt.setLong(4, drbdRscDataRef.getAlStripeSize());
            stmt.setLong(5, drbdRscDataRef.getFlags().getFlagsBits(dbCtx));
            stmt.setInt(6, drbdRscDataRef.getNodeId().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscData created %s", getId(drbdRscDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void persist(DrbdRscDfnData<?> drbdRscDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdRscDfnData %s", getId(drbdRscDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_RSC_DFN))
        {
            stmt.setString(1, drbdRscDfnDataRef.getResourceName().value);
            stmt.setString(2, drbdRscDfnDataRef.getRscNameSuffix());
            SnapshotName snapName = drbdRscDfnDataRef.getSnapshotName();
            if (snapName == null)
            {
                stmt.setString(3, DFLT_SNAP_NAME_FOR_RSC);
            }
            else
            {
                stmt.setString(3, snapName.value);
            }
            stmt.setShort(4, drbdRscDfnDataRef.getPeerSlots());
            stmt.setInt(5, drbdRscDfnDataRef.getAlStripes());
            stmt.setLong(6, drbdRscDfnDataRef.getAlStripeSize());
            TcpPortNumber tcpPort = drbdRscDfnDataRef.getTcpPort();
            if (tcpPort == null)
            {
                stmt.setNull(7, Types.INTEGER);
            }
            else
            {
                stmt.setInt(7, tcpPort.value);
            }
            stmt.setString(8, drbdRscDfnDataRef.getTransportType().name());
            if (drbdRscDfnDataRef.getSecret() != null)
            {
                stmt.setString(9, drbdRscDfnDataRef.getSecret());
            }
            else
            {
                stmt.setNull(9, Types.VARCHAR);
            }

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDfnData created %s", getId(drbdRscDfnDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void persist(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdVlmData %s", getId(drbdVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, drbdVlmDataRef.getRscLayerId());
            stmt.setInt(2, drbdVlmDataRef.getVlmNr().value);
            StorPool externalMetaDataStorPool = drbdVlmDataRef.getExternalMetaDataStorPool();
            if (externalMetaDataStorPool != null)
            {
                stmt.setString(3, externalMetaDataStorPool.getNode().getName().value);
                stmt.setString(4, externalMetaDataStorPool.getName().value);
            }
            else
            {
                stmt.setNull(3, Types.VARCHAR);
                stmt.setNull(4, Types.VARCHAR);
            }

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmData created %s", getId(drbdVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void persist(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM_DFN))
        {
            stmt.setString(1, drbdVlmDfnDataRef.getResourceName().value);
            stmt.setString(2, drbdVlmDfnDataRef.getRscNameSuffix());
            SnapshotName snapName = drbdVlmDfnDataRef.getSnapshotName();
            if (snapName == null)
            {
                stmt.setString(3, DFLT_SNAP_NAME_FOR_RSC);
            }
            else
            {
                stmt.setString(3, snapName.value);
            }
            stmt.setInt(4, drbdVlmDfnDataRef.getVolumeNumber().value);
            MinorNumber minorNr = drbdVlmDfnDataRef.getMinorNr();
            if (minorNr == null)
            {
                stmt.setNull(5, Types.INTEGER);
            }
            else
            {
                stmt.setInt(5, minorNr.value);
            }

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmDfnData created %s", getId(drbdVlmDfnDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdRscData<?> drbdRscDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDataRef %s", getId(drbdRscDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC))
        {
            stmt.setInt(1, drbdRscDataRef.getRscLayerId());

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDataRef deleted %s", getId(drbdRscDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdRscDfnData<?> drbdRscDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDfnData %s", getId(drbdRscDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC_DFN))
        {
            stmt.setString(1, drbdRscDfnDataRef.getResourceName().value);
            stmt.setString(2, drbdRscDfnDataRef.getRscNameSuffix());
            SnapshotName snapName = drbdRscDfnDataRef.getSnapshotName();
            if (snapName == null)
            {
                stmt.setString(3, DFLT_SNAP_NAME_FOR_RSC);
            }
            else
            {
                stmt.setString(3, snapName.value);
            }

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDfnData deleted %s", getId(drbdRscDfnDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmData %s", getId(drbdVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, drbdVlmDataRef.getRscLayerId());
            stmt.setInt(2, drbdVlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmData deleted %s", getId(drbdVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM_DFN))
        {
            stmt.setString(1, drbdVlmDfnDataRef.getResourceName().value);
            stmt.setString(2, drbdVlmDfnDataRef.getRscNameSuffix());
            SnapshotName snapName = drbdVlmDfnDataRef.getSnapshotName();
            if (snapName == null)
            {
                stmt.setString(3, DFLT_SNAP_NAME_FOR_RSC);
            }
            else
            {
                stmt.setString(3, snapName.value);
            }
            stmt.setInt(4, drbdVlmDfnDataRef.getVolumeNumber().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmDfnData deleted %s", getId(drbdVlmDfnDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver()
    {
        return rscDfnSecretDriver;
    }

    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return rscStatePersistence;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver()
    {
        return rscNodeIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver()
    {
        return vlmExtStorPoolDriver;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver()
    {
        return rscDfnTcpPortDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver()
    {
        return rscDfnTransportTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver()
    {
        return rscDfnPeerSlotsDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private class RscFlagsDriver implements StateFlagsPersistence<DrbdRscData<?>>
    {
        @Override
        public void persist(DrbdRscData<?> drbdRscData, long flags)
            throws DatabaseException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        DrbdRscObject.DrbdRscFlags.class,
                        drbdRscData.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        DrbdRscObject.DrbdRscFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating DrbdRscData's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(drbdRscData)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setLong(2, drbdRscData.getRscLayerId());
                    stmt.executeUpdate();
                }
                catch (SQLException sqlExc)
                {
                    throw new DatabaseException(sqlExc);
                }
                errorReporter.logTrace(
                    "DrbdRscData's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(drbdRscData)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class RscNodeIdDriver implements SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId>
    {

        @Override
        public void update(DrbdRscData<?> drbdRscData, NodeId newNodeId) throws DatabaseException
        {
            String fromStr = Integer.toString(drbdRscData.getNodeId().value);
            String toStr = Integer.toString(newNodeId.value);

            errorReporter.logTrace(
                "Updating DrbdRscData's node id from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(drbdRscData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_NODE_ID))
            {
                stmt.setInt(1, newNodeId.value);
                stmt.setLong(2, drbdRscData.getRscLayerId());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdVlmData's external storage pool updated from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(drbdRscData)
            );
        }
    }

    private class VlmExtStorPoolDriver implements SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool>
    {
        @Override
        public void update(DrbdVlmData<?> drbdVlmData, StorPool storPool) throws DatabaseException
        {
            String fromStr = null;
            String toStr = null;
            if (drbdVlmData.getExternalMetaDataStorPool() != null)
            {
                fromStr = drbdVlmData.getExternalMetaDataStorPool().getName().displayValue;
            }
            if (storPool != null)
            {
                toStr = storPool.getName().displayValue;
            }
            errorReporter.logTrace(
                "Updating DrbdVlmData's external storage pool from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(drbdVlmData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_VLM_EXT_STOR_POOL))
            {
                if (storPool == null)
                {
                    stmt.setNull(1, Types.VARCHAR);
                    stmt.setNull(2, Types.VARCHAR);
                }
                else
                {
                    stmt.setString(1, storPool.getNode().getName().value);
                    stmt.setString(2, storPool.getName().value);
                }

                stmt.setLong(3, drbdVlmData.getRscLayerId());
                stmt.setInt(4, drbdVlmData.getVlmNr().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdVlmData's external storage pool updated from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(drbdVlmData)
            );
        }
    }

    private class RscDfnSecretDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String>
    {
        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, String secretRef)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's secret from [%s] to [%s] %s",
                drbdRscDfnData.getSecret(),
                secretRef,
                getId(drbdRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_DFN_SECRET))
            {
                stmt.setString(1, secretRef);
                stmt.setString(2, drbdRscDfnData.getResourceName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                SnapshotName snapName = drbdRscDfnData.getSnapshotName();
                if (snapName == null)
                {
                    stmt.setString(4, DFLT_SNAP_NAME_FOR_RSC);
                }
                else
                {
                    stmt.setString(4, snapName.displayValue);
                }
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's secret updated from [%s] to [%s] %s",
                drbdRscDfnData.getSecret(),
                secretRef,
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnTcpPortDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber>
    {
        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, TcpPortNumber port)
            throws DatabaseException
        {
            TcpPortNumber tcpPort = drbdRscDfnData.getTcpPort();
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's port from [%d] to [%d] %s",
                tcpPort,
                port,
                getId(drbdRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_DFN_TCP_PORT))
            {
                stmt.setInt(1, port.value);
                stmt.setString(2, drbdRscDfnData.getResourceName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                SnapshotName snapName = drbdRscDfnData.getSnapshotName();
                if (snapName == null)
                {
                    stmt.setString(4, DFLT_SNAP_NAME_FOR_RSC);
                }
                else
                {
                    stmt.setString(4, snapName.displayValue);
                }
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's port updated from [%d] to [%d] %s",
                drbdRscDfnData.getTcpPort(),
                port,
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnTransportTypeDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType>
    {
        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, TransportType transportType)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's transport type from [%s] to [%s] %s",
                drbdRscDfnData.getTransportType().name(),
                transportType.name(),
                getId(drbdRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_DFN_TRANSPORT_TYPE))
            {
                stmt.setString(1, transportType.name());
                stmt.setString(2, drbdRscDfnData.getResourceName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                SnapshotName snapName = drbdRscDfnData.getSnapshotName();
                if (snapName == null)
                {
                    stmt.setString(4, DFLT_SNAP_NAME_FOR_RSC);
                }
                else
                {
                    stmt.setString(4, snapName.displayValue);
                }
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's transport type updated from [%d] to [%d] %s",
                drbdRscDfnData.getTransportType().name(),
                transportType.name(),
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnPeerSlotsDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short>
    {
        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, Short peerSlots)
            throws DatabaseException
        {
            short oldPeerSlots = drbdRscDfnData.getPeerSlots();
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's peer slots from [%d] to [%d] %s",
                oldPeerSlots,
                peerSlots,
                getId(drbdRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_DFN_PEER_SLOTS))
            {
                stmt.setShort(1, peerSlots);
                stmt.setString(2, drbdRscDfnData.getResourceName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                SnapshotName snapName = drbdRscDfnData.getSnapshotName();
                if (snapName == null)
                {
                    stmt.setString(4, DFLT_SNAP_NAME_FOR_RSC);
                }
                else
                {
                    stmt.setString(4, snapName.displayValue);
                }
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's peer slots updated from [%d] to [%d] %s",
                oldPeerSlots,
                peerSlots,
                getId(drbdRscDfnData)
            );
        }
    }
}
