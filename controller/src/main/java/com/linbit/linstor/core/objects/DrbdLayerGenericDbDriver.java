package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
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
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

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
            "    RD." + RESOURCE_NAME_SUFFIX + " = VD." + RESOURCE_NAME_SUFFIX;
    // DrbdVlmData has nothing we have to store -> no database table

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
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final RscFlagsDriver rscStatePersistence;
    private final VlmExtStorPoolDriver vlmExtStorPoolDriver;
    private final RscDfnSecretDriver rscDfnSecretDriver;
    private final RscDfnTcpPortDriver rscDfnTcpPortDriver;
    private final RscDfnTransportTypeDriver rscDfnTransportTypeDriver;
    private final RscDfnPeerSlotsDriver rscDfnPeerSlotsDriver;

    private final Map<Pair<ResourceDefinition, String>, Pair<DrbdRscDfnData, List<DrbdRscData>>> drbdRscDfnCache;
    private final Map<Pair<VolumeDefinition, String>, DrbdVlmDfnData> drbdVlmDfnCache;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    @Inject
    public DrbdLayerGenericDbDriver(
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
        vlmExtStorPoolDriver = new VlmExtStorPoolDriver();
        rscDfnSecretDriver = new RscDfnSecretDriver();
        rscDfnTcpPortDriver = new RscDfnTcpPortDriver();
        rscDfnTransportTypeDriver = new RscDfnTransportTypeDriver();
        rscDfnPeerSlotsDriver = new RscDfnPeerSlotsDriver();

        drbdRscDfnCache = new HashMap<>();
        drbdVlmDfnCache = new HashMap<>();
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
    public void clearLoadCache()
    {
        drbdRscDfnCache.clear();
        drbdVlmDfnCache.clear();
    }


    /**
     * Loads the layer data for the given {@link ResourceDefinition}s if available.
     *
     * @param rscDfnMap
     * @throws DatabaseException
     */
    @Override
    public void loadLayerData(Map<ResourceName, ResourceDefinition> rscDfnMap) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RSC_DFN_AND_VLM_DFN))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ResourceName rscName = getResourceName(resultSet, "RD_" + RESOURCE_NAME);
                    ResourceDefinition rscDfn = rscDfnMap.get(rscName);
                    if (rscDfn == null)
                    {
                        throw new LinStorDBRuntimeException(
                            "Loaded drbd resource definition data for non existent resource definition '" +
                                rscName + "'"
                        );
                    }
                    String rscNameSuffix = resultSet.getString("RD_" + RESOURCE_NAME_SUFFIX);
                    DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(dbCtx, DeviceLayerKind.DRBD, rscNameSuffix);
                    if (drbdRscDfnData == null)
                    {
                        short peerSlots = resultSet.getShort("RD_" + PEER_SLOTS);
                        int alStripes = resultSet.getInt("RD_" + AL_STRIPES);
                        long alStripeSize = resultSet.getLong("RD_" + AL_STRIPE_SIZE);
                        int tcpPort = resultSet.getInt("RD_" + TCP_PORT);

                        TransportType transportType = TransportType.byValue(
                            resultSet.getString("RD_" + TRANSPORT_TYPE)
                        );
                        String secret = resultSet.getString("RD_" + SECRET);

                        List<DrbdRscData> rscDataList = new ArrayList<>();

                        drbdRscDfnData = new DrbdRscDfnData(
                            rscDfn,
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
                        Pair<DrbdRscDfnData, List<DrbdRscData>> pair = new Pair<>(drbdRscDfnData, rscDataList);
                        drbdRscDfnCache.put(new Pair<>(rscDfn, rscNameSuffix), pair);

                        rscDfn.setLayerData(dbCtx, drbdRscDfnData);
                    }

                    // drbdRscDfnData is now restored. If "VD_" columns are not empty, restore the DrbdVlmDfnData
                    Integer vlmNr = resultSet.getInt("VD_" + VLM_NR);
                    if (!resultSet.wasNull())
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
                        DrbdVlmDfnData drbdVlmDfnData = new DrbdVlmDfnData(
                            vlmDfn,
                            rscNameSuffix,
                            minor,
                            minorPool,
                            (DrbdRscDfnData) vlmDfn.getResourceDefinition().getLayerData(
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

    /**
     * Fully loads a {@link DrbdRscData} object, including the {@link DrbdRscDfnData}, {@link DrbdVlmData} and
     * {@link DrbdVlmDfnData}
     * @param parentRef
     * @param storPoolMapRef
     *
     * @return a {@link Pair}, where the first object is the actual DrbdRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    @Override
    public Pair<DrbdRscData, Set<RscLayerObject>> load(
        Resource rsc,
        int id,
        String rscSuffixRef,
        RscLayerObject parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> storPoolMapRef
    )
        throws DatabaseException
    {
        Pair<DrbdRscData, Set<RscLayerObject>> ret;
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_RSC_BY_ID))
        {
            stmt.setInt(1, id);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    Pair<DrbdRscDfnData, List<DrbdRscData>> drbdRscDfnDataPair = drbdRscDfnCache.get(
                        new Pair<>(rsc.getDefinition(), rscSuffixRef)
                    );
                    Set<RscLayerObject> children = new HashSet<>();
                    Map<VolumeNumber, DrbdVlmData> vlmMap = new TreeMap<>();

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

                    restoreDrbdVolumes(ret.objA, vlmMap, storPoolMapRef);
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

    private void restoreDrbdVolumes(
        DrbdRscData rscData,
        Map<VolumeNumber, DrbdVlmData> vlmMap,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> storPoolMapRef
    )
        throws DatabaseException
    {
        Resource rsc = rscData.getResource();
        NodeName currentNodeName = rsc.getAssignedNode().getName();

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

                    VolumeNumber vlmNr;
                    vlmNr = new VolumeNumber(vlmNrInt);

                    Volume vlm = rsc.getVolume(vlmNr);

                    VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                    DrbdVlmDfnData drbdVlmDfnData = drbdVlmDfnCache.get(
                        new Pair<>(vlmDfn, rscData.getResourceNameSuffix())
                    );

                    StorPool extMetaDataStorPool = null;
                    if (extMetaStorPoolNameStr != null)
                    {
                        StorPoolName extStorPoolName = new StorPoolName(extMetaStorPoolNameStr);
                        extMetaDataStorPool = storPoolMapRef.get(
                            new Pair<>(currentNodeName, extStorPoolName)
                        ).objA;
                    }

                    if (drbdVlmDfnData != null)
                    {
                        vlmMap.put(
                            vlm.getVolumeDefinition().getVolumeNumber(),
                            new DrbdVlmData(
                                vlm,
                                rscData,
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
                        throw new ImplementationError("Trying to load drbdVlm without drbdVlmDfn. " +
                            "layerId: " + rscData.getRscLayerId() + " node name: " + currentNodeName +
                            " resource name: " + rsc.getDefinition().getName() + " vlmNr " + vlmNrInt);
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
    public void create(DrbdRscData drbdRscDataRef) throws DatabaseException
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
    public void persist(DrbdRscDfnData drbdRscDfnDataRef) throws DatabaseException
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void persist(DrbdVlmData drbdVlmDataRef) throws DatabaseException
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
    public void persist(DrbdVlmDfnData drbdVlmDfnDataRef) throws DatabaseException
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdRscData drbdRscDataRef) throws DatabaseException
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
    public void delete(DrbdRscDfnData drbdRscDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDfnData %s", getId(drbdRscDfnDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC_DFN))
        {
            stmt.setString(1, drbdRscDfnDataRef.getResourceDefinition().getName().value);
            stmt.setString(2, drbdRscDfnDataRef.getRscNameSuffix());

            stmt.executeUpdate();
            errorReporter.logTrace("DrbdRscDfnData deleted %s", getId(drbdRscDfnDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(DrbdVlmData drbdVlmDataRef) throws DatabaseException
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
    public void delete(DrbdVlmDfnData drbdVlmDfnDataRef) throws DatabaseException
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
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
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
    public SingleColumnDatabaseDriver<DrbdVlmData, StorPool> getExtStorPoolDriver()
    {
        return vlmExtStorPoolDriver;
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

    private String getId(DrbdVlmData drbdVlmData)
    {
        return "(LayerRscId=" + drbdVlmData.getRscLayerId() +
            ", VlmNr=" + drbdVlmData.getVlmNr() +
            ")";
    }

    private String getId(DrbdRscData drbdRscData)
    {
        return "(LayerRscId=" + drbdRscData.getRscLayerId() +
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

    private class VlmExtStorPoolDriver implements SingleColumnDatabaseDriver<DrbdVlmData, StorPool>
    {
        @Override
        public void update(DrbdVlmData drbdVlmData, StorPool storPool) throws DatabaseException
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

    private class RscDfnSecretDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, String>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, String secretRef)
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
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

    private class RscDfnTcpPortDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, TcpPortNumber port)
            throws DatabaseException
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
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
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

    private class RscDfnPeerSlotsDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData, Short>
    {
        @Override
        public void update(DrbdRscDfnData drbdRscDfnData, Short peerSlots)
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
                stmt.setString(2, drbdRscDfnData.getResourceDefinition().getName().value);
                stmt.setString(3, drbdRscDfnData.getRscNameSuffix());
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
