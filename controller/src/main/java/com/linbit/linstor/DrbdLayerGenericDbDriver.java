package com.linbit.linstor;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
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
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.AL_STRIPES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.AL_STRIPE_SIZE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.FLAGS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.PEER_SLOTS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.SECRET;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TCP_PORT;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TRANSPORT_TYPE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_RESOURCES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_RESOURCE_DEFINITIONS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_DRBD_VOLUME_DEFINITIONS;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("checkstyle:magicnumber")
@Singleton
public class DrbdLayerGenericDbDriver implements DrbdLayerDatabaseDriver
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
    private static final String[] RSC_DFN_ALL_FIELDS =
    {
        RESOURCE_NAME,
        RESOURCE_NAME_SUFFIX,
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
        VLM_NR,
        VLM_MINOR_NR
    };

    private static final String SELECT_RSC_BY_ID =
        " SELECT " + StringUtils.join(", ", RSC_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_RESOURCES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String SELECT_RSC_DFN_BY_NAME =
        " SELECT " + StringUtils.join(", ", RSC_DFN_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    // DrbdVlmData has nothing we have to store -> no database table
    private static final String SELECT_VLM_DFN_BY_RSC_NAME_AND_VLM_NR =
        " SELECT " + StringUtils.join(", ", VLM_DFN_ALL_FIELDS) +
        " FROM " + TBL_LAYER_DRBD_VOLUME_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    VLM_NR               + " = ?";

    private static final String INSERT_RSC =
        " INSERT INTO " + TBL_LAYER_DRBD_RESOURCES +
        " ( " + StringUtils.join(", ", RSC_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", RSC_ALL_FIELDS.length) + " )";
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
    private static final String DELETE_RSC_DFN =
        " DELETE FROM " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    private static final String DELETE_VLM_DFN =
        " DELETE FROM " + TBL_LAYER_DRBD_VOLUME_DEFINITIONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ? AND " +
                    VLM_NR               + " = ?";

    private static final String UPDATE_RSC_FLAGS =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCES +
        " SET " + FLAGS + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";
    private static final String UPDATE_RSC_DFN_SECRET =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + SECRET + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    private static final String UPDATE_RSC_DFN_TCP_PORT =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + TCP_PORT + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    private static final String UPDATE_RSC_DFN_TRANSPORT_TYPE =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + TRANSPORT_TYPE + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    private static final String UPDATE_RSC_DFN_PEER_SLOTS =
        " UPDATE " + TBL_LAYER_DRBD_RESOURCE_DEFINITIONS +
        " SET " + PEER_SLOTS + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    private final RscFlagsDriver rscStatePersistence;
    private final RscDfnSecretDriver rscDfnSecretDriver;
    private final RscDfnTcpPortDriver rscDfnTcpPortDriver;
    private final RscDfnTransportTypeDriver rscDfnTransportTypeDriver;
    private final RscDfnPeerSlotsDriver rscDfnPeerSlotsDriver;

    private final Map<ResourceDefinition, Pair<DrbdRscDfnData, List<DrbdRscData>>> drbdRscDfnCache;
    private final Map<VolumeDefinition, DrbdVlmDfnData> drbdVlmDfnCache;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    @Inject
    public DrbdLayerGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
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
        rscDfnSecretDriver = new RscDfnSecretDriver();
        rscDfnTcpPortDriver = new RscDfnTcpPortDriver();
        rscDfnTransportTypeDriver = new RscDfnTransportTypeDriver();
        rscDfnPeerSlotsDriver = new RscDfnPeerSlotsDriver();

        drbdRscDfnCache = new HashMap<>();
        drbdVlmDfnCache = new HashMap<>();
    }

    /*
     * These caches are only used during loading. Having them cleared after having loaded all data
     * these caches should never be used again.
     */
    public void clearLoadCache()
    {
        drbdRscDfnCache.clear();
        drbdVlmDfnCache.clear();
    }

    /**
     * Fully loads a {@link DrbdRscData} object, including the {@link DrbdRscDfnData}, {@link DrbdVlmData} and
     * {@link DrbdVlmDfnData}
     * @param parentRef
     *
     * @return a {@link Pair}, where the first object is the actual DrbdRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws SQLException
     * @throws AccessDeniedException
     */
    public Pair<DrbdRscData, Set<RscLayerObject>> load(
        Resource rsc,
        int id,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws SQLException, AccessDeniedException
    {
        Pair<DrbdRscData, Set<RscLayerObject>> ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_RSC_BY_ID))
        {
            stmt.setInt(1, id);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    Pair<DrbdRscDfnData, List<DrbdRscData>> drbdRscDfnDataPair = loadRscDfnData(rsc, rscSuffixRef);
                    Set<RscLayerObject> children = new HashSet<>();
                    Map<VolumeNumber, DrbdVlmData> vlmMap = new TreeMap<>();

                    NodeId nodeId;
                    try
                    {
                        nodeId = new NodeId(resultSet.getInt(NODE_ID));
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new LinStorSqlRuntimeException(
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

                    ret = new Pair<DrbdRscData, Set<RscLayerObject>>(
                        new DrbdRscData(
                            id,
                            rsc,
                            parentRef,
                            drbdRscDfnDataPair.objA,
                            children,
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
                        ),
                        children
                    );
                    drbdRscDfnDataPair.objB.add(ret.objA);

                    restoreDrbdVolumes(ret.objA, vlmMap);
                }
                else
                {
                    throw new ImplementationError("Requested id [" + id + "] was not found in the database");
                }
            }
            catch (ValueOutOfRangeException | ExhaustedPoolException | ValueInUseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return ret;
    }

    private Pair<DrbdRscDfnData, List<DrbdRscData>> loadRscDfnData(Resource rscRef, String rscSuffix)
        throws SQLException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        DrbdRscDfnData drbdRscDfnData;

        ResourceDefinition rscDfn = rscRef.getDefinition();
        Pair<DrbdRscDfnData, List<DrbdRscData>> ret = drbdRscDfnCache.get(rscDfn);
        if (ret == null)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_RSC_DFN_BY_NAME))
            {
                stmt.setString(1, rscDfn.getName().value);
                stmt.setString(2, rscSuffix);

                try (ResultSet resultSet = stmt.executeQuery())
                {
                    List<DrbdRscData> rscDataList = new ArrayList<>();

                    if (resultSet.next())
                    {
                        short peerSlots = resultSet.getShort(PEER_SLOTS);
                        int alStripes = resultSet.getInt(AL_STRIPES);
                        long alStripeSize = resultSet.getLong(AL_STRIPE_SIZE);

                        TransportType transportType = TransportType.byValue(resultSet.getString(TRANSPORT_TYPE));
                        String secret = resultSet.getString(SECRET);

                        drbdRscDfnData =
                            new DrbdRscDfnData(
                                rscDfn,
                                rscSuffix,
                                peerSlots,
                                alStripes,
                                alStripeSize,
                                resultSet.getInt(TCP_PORT),
                                transportType,
                                secret,
                                rscDataList,
                                new TreeMap<>(),
                                tcpPortPool,
                                this,
                                transObjFactory,
                                transMgrProvider
                            );
                        ret = new Pair<>(drbdRscDfnData, rscDataList);
                        drbdRscDfnCache.put(rscDfn, ret);

                        rscDfn.setLayerData(dbCtx, drbdRscDfnData);
                    }
                    else
                    {
                        throw new LinStorSqlRuntimeException(
                            "Required DrbdResourceDefinition does not exist. " +
                            rscDfn.getName() + " " + rscSuffix
                        );
                    }
                }
            }
        }
        return ret;
    }

    private void restoreDrbdVolumes(DrbdRscData rscData, Map<VolumeNumber, DrbdVlmData> vlmMap)
        throws SQLException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        Iterator<Volume> vlmIt = rscData.getResource().iterateVolumes();
        while (vlmIt.hasNext())
        {
            Volume vlm = vlmIt.next();

            DrbdVlmDfnData drbdVlmDfnData = loadDrbdVlmDfnData(
                vlm.getVolumeDefinition(),
                rscData.getResourceNameSuffix()
            );

            if (drbdVlmDfnData != null)
            {
                vlmMap.put(
                    vlm.getVolumeDefinition().getVolumeNumber(),
                    new DrbdVlmData(
                        vlm,
                        rscData,
                        drbdVlmDfnData,
                        transObjFactory,
                        transMgrProvider
                    )
                );
            }
        }
    }

    private DrbdVlmDfnData loadDrbdVlmDfnData(VolumeDefinition vlmDfn, String rscNameSuffix)
        throws SQLException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        DrbdVlmDfnData drbdVlmDfnData = drbdVlmDfnCache.get(vlmDfn);

        if (drbdVlmDfnData == null)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_VLM_DFN_BY_RSC_NAME_AND_VLM_NR))
            {
                stmt.setString(1, vlmDfn.getResourceDefinition().getName().value);
                stmt.setString(2, rscNameSuffix);
                stmt.setInt(3, vlmDfn.getVolumeNumber().value);
                try (ResultSet resultSet = stmt.executeQuery())
                {
                    if (resultSet.next())
                    {
                        drbdVlmDfnData = new DrbdVlmDfnData(
                            vlmDfn,
                            rscNameSuffix,
                            resultSet.getInt(VLM_MINOR_NR),
                            minorPool,
                            (DrbdRscDfnData) vlmDfn.getResourceDefinition().getLayerData(
                                dbCtx,
                                DeviceLayerKind.DRBD,
                                rscNameSuffix
                            ),
                            this,
                            transMgrProvider
                        );
                        drbdVlmDfnCache.put(vlmDfn, drbdVlmDfnData);

                        vlmDfn.setLayerData(dbCtx, drbdVlmDfnData);
                    }
                    // else: this volume definition is not a drbd volume (or we lost some data)
                }
            }
        }

        return drbdVlmDfnData;
    }

    @Override
    public void create(DrbdRscData drbdRscDataRef) throws SQLException
    {
        @SuppressWarnings("resource") // will be done in DbConnectionPool#returnConnection
        Connection connection = getConnection();

        errorReporter.logTrace("Creating DrbdRscData %s", getId(drbdRscDataRef));
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_RSC))
        {
            stmt.setLong(1, drbdRscDataRef.getRscLayerId());
            stmt.setShort(2, drbdRscDataRef.getPeerSlots());
            stmt.setInt(3, drbdRscDataRef.getAlStripes());
            stmt.setLong(4, drbdRscDataRef.getAlStripeSize());
            stmt.setLong(5, drbdRscDataRef.getFlags().getFlagsBits(dbCtx));
            stmt.setInt(6, drbdRscDataRef.getNodeId().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscData created %s", getId(drbdRscDataRef));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void persist(DrbdRscDfnData drbdRscDfnDataRef) throws SQLException
    {
        errorReporter.logTrace("Creating DrbdRscDfnData %s", getId(drbdRscDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_RSC_DFN))
        {
            stmt.setString(1, drbdRscDfnDataRef.getResourceDefinition().getName().value);
            stmt.setString(2, drbdRscDfnDataRef.getRscNameSuffix());
            stmt.setShort(3, drbdRscDfnDataRef.getPeerSlots());
            stmt.setInt(4, drbdRscDfnDataRef.getAlStripes());
            stmt.setLong(5, drbdRscDfnDataRef.getAlStripeSize());
            stmt.setInt(6, drbdRscDfnDataRef.getTcpPort().value);
            stmt.setString(7, drbdRscDfnDataRef.getTransportType().name());
            stmt.setString(8, drbdRscDfnDataRef.getSecret());

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDfnData created %s", getId(drbdRscDfnDataRef));
        }
    }

    @Override
    public void persist(DrbdVlmData drbdVlmDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if DrbdVlmData will get a database table in future.
    }

    @Override
    public void persist(DrbdVlmDfnData drbdVlmDfnDataRef) throws SQLException
    {
        errorReporter.logTrace("Creating DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM_DFN))
        {
            stmt.setString(1, drbdVlmDfnDataRef.getVolumeDefinition().getResourceDefinition().getName().value);
            stmt.setString(2, drbdVlmDfnDataRef.getRscNameSuffix());
            stmt.setInt(3, drbdVlmDfnDataRef.getVolumeDefinition().getVolumeNumber().value);
            stmt.setInt(4, drbdVlmDfnDataRef.getMinorNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmDfnData created %s", getId(drbdVlmDfnDataRef));
        }
    }

    @Override
    public void delete(DrbdRscData drbdRscDataRef) throws SQLException
    {
        errorReporter.logTrace("Deleting DrbdRscDataRef %s", getId(drbdRscDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC))
        {
            stmt.setLong(1, drbdRscDataRef.getRscLayerId());

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDataRef deleted %s", getId(drbdRscDataRef));
        }
    }

    @Override
    public void delete(DrbdRscDfnData drbdRscDfnDataRef) throws SQLException
    {
        errorReporter.logTrace("Deleting DrbdRscDfnData %s", getId(drbdRscDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC_DFN))
        {
            stmt.setString(1, drbdRscDfnDataRef.getResourceDefinition().getName().value);
            stmt.setString(2, drbdRscDfnDataRef.getRscNameSuffix());

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDfnData deleted %s", getId(drbdRscDfnDataRef));
        }
    }

    @Override
    public void delete(DrbdVlmData drbdVlmDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if DrbdVlmData will get a database table in future.
    }

    @Override
    public void delete(DrbdVlmDfnData drbdVlmDfnDataRef) throws SQLException
    {
        errorReporter.logTrace("Deleting DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM_DFN))
        {
            stmt.setString(1, drbdVlmDfnDataRef.getVolumeDefinition().getResourceDefinition().getName().value);
            stmt.setString(2, drbdVlmDfnDataRef.getRscNameSuffix());
            stmt.setInt(3, drbdVlmDfnDataRef.getVolumeDefinition().getVolumeNumber().value);

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdVlmDfnData deleted %s", getId(drbdVlmDfnDataRef));
        }
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, String> getRscDfnSecretDriver()
    {
        return rscDfnSecretDriver;
    }

    @Override
    public StateFlagsPersistence<DrbdRscData> getRscStateFlagPersistence()
    {
        return rscStatePersistence;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getTcpPortDriver()
    {
        return rscDfnTcpPortDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver()
    {
        return rscDfnTransportTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, Short> getPeerSlotsDriver()
    {
        return rscDfnPeerSlotsDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(DrbdRscData drbdRscData)
    {
        return "(LayerRscId=" + drbdRscData.getRscLayerId() +
            ", SuffResName=" + drbdRscData.getSuffixedResourceName() +
            ")";
    }

    private String getId(DrbdRscDfnData drbdRscDfnData)
    {
        return "(SuffResName=" + drbdRscDfnData.getSuffixedResourceName() + ")";
    }

    private String getId(DrbdVlmDfnData drbdVlmDfnData)
    {
        return "(SuffResName=" + drbdVlmDfnData.getSuffixedResourceName() +
            ", VlmNr=" + drbdVlmDfnData.getVolumeDefinition().getVolumeNumber().value +
            ")";
    }

    private class RscFlagsDriver implements StateFlagsPersistence<DrbdRscData>
    {
        @Override
        public void persist(DrbdRscData drbdRscData, long flags)
            throws SQLException
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
                errorReporter.logTrace(
                    "DrbdRscData's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(drbdRscData)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class RscDfnSecretDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, String>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, String secretRef)
            throws SQLException
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                stmt.executeUpdate();
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's secret updated from [%s] to [%s] %s",
                drbdRscDfnData.getSecret(),
                secretRef,
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnTcpPortDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, TcpPortNumber port)
            throws SQLException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's port from [%d] to [%d] %s",
                drbdRscDfnData.getTcpPort().value,
                port,
                getId(drbdRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_RSC_DFN_TCP_PORT))
            {
                stmt.setInt(1, port.value);
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                stmt.executeUpdate();
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's port updated from [%d] to [%d] %s",
                drbdRscDfnData.getTcpPort().value,
                port,
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnTransportTypeDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, TransportType transportType)
            throws SQLException
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                stmt.executeUpdate();
            }
            errorReporter.logTrace(
                "DrbdRscDfnData's transport type updated from [%d] to [%d] %s",
                drbdRscDfnData.getTransportType().name(),
                transportType.name(),
                getId(drbdRscDfnData)
            );
        }
    }

    private class RscDfnPeerSlotsDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, Short>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, Short peerSlots)
            throws SQLException
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
                stmt.executeUpdate();
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
