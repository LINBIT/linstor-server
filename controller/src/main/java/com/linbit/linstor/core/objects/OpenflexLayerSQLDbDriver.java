package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NQN;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.RESOURCE_NAME_SUFFIX;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_OPENFLEX_RESOURCE_DEFINITONS;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_OPENFLEX_VOLUMES;
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
public class OpenflexLayerSQLDbDriver implements OpenflexLayerCtrlDatabaseDriver
{
    private static final String[] RSC_DFN_ALL_FIELDS =
    {
        RESOURCE_NAME,
        // SNAPSHOT_NAME, // snapshots are currently not supported
        RESOURCE_NAME_SUFFIX,
        NQN
    };

    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        NODE_NAME,
        POOL_NAME
    };

    private static final String SELECT_ALL_RSC_DFN =
        " SELECT " + StringUtils.join(", ", RSC_DFN_ALL_FIELDS) +
        " FROM " + TBL_LAYER_OPENFLEX_RESOURCE_DEFINITONS;
    private static final String SELECT_ALL_VLMS =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_OPENFLEX_VOLUMES;

    private static final String INSERT_RSC_DFN =
        " INSERT INTO " + TBL_LAYER_OPENFLEX_RESOURCE_DEFINITONS +
        " ( " + StringUtils.join(", ", RSC_DFN_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ",", RSC_DFN_ALL_FIELDS.length) + " )";
    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_OPENFLEX_VOLUMES +
        " ( " + StringUtils.join(", ", VLM_ALL_FIELDS) + " ) " +
        " VALUES ( " + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) + ")";

    private static final String DELETE_RSC_DFN =
        " DELETE FROM " + TBL_LAYER_OPENFLEX_RESOURCE_DEFINITONS +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";
    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_OPENFLEX_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ?";

    public static final  String UPDATE_NQN =
        " UPDATE " + TBL_LAYER_OPENFLEX_RESOURCE_DEFINITONS +
        " SET " + NQN + " = ? " +
        " WHERE " + RESOURCE_NAME        + " = ? AND " +
                    RESOURCE_NAME_SUFFIX + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final NameShortener nameShortener;

    private final NqnDriver nqnDriver;

    // key is layerRscId
    private Map<Integer, List<OpenflexVlmInfo>> cachedVlmInfoMap;
    private Map<Pair<ResourceDefinition, String>, Pair<OpenflexRscDfnData<Resource>, ArrayList<OpenflexRscData<Resource>>>> cacheRscDfnDataMap;


    @Inject
    public OpenflexLayerSQLDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef,
        @Named(NameShortener.OPENFLEX) NameShortener nameShortenerRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nameShortener = nameShortenerRef;

        nqnDriver = new NqnDriver();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException
    {
        return nqnDriver;
    }

    @Override
    public void clearLoadAllCache()
    {
        cachedVlmInfoMap.clear();
        cachedVlmInfoMap = null;

        cacheRscDfnDataMap.clear();
        cacheRscDfnDataMap = null;
    }

    @Override
    public void fetchForLoadAll(
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMap
    )
        throws DatabaseException
    {
        fetchOfRscDfns(rscDfnMap);
        fetchOfVlms(tmpStorPoolMapRef);
    }

    private void fetchOfRscDfns(Map<ResourceName, ResourceDefinition> rscDfnMap) throws DatabaseException
    {
        cacheRscDfnDataMap = new HashMap<>();
        try (
            PreparedStatement loadAllOfRscDfnStmt = getConnection().prepareStatement(SELECT_ALL_RSC_DFN);
            ResultSet resultSet = loadAllOfRscDfnStmt.executeQuery();
        )
        {
            while (resultSet.next())
            {
                String rscNameStr = resultSet.getString(RESOURCE_NAME);
                String rscNameSuffix = resultSet.getString(RESOURCE_NAME_SUFFIX);

                ResourceName rscName = new ResourceName(rscNameStr);

                ResourceDefinition rscDfn = rscDfnMap.get(rscName);
                OpenflexRscDfnData<Resource> ofRscDfnData = rscDfn.getLayerData(
                    dbCtx,
                    DeviceLayerKind.OPENFLEX,
                    rscNameSuffix
                );

                if (ofRscDfnData == null)
                {
                    String nqn = resultSet.getString(NQN);

                    ArrayList<OpenflexRscData<Resource>> rscDataList = new ArrayList<>();

                    try
                    {
                        ofRscDfnData = new OpenflexRscDfnData<>(
                            rscDfn.getName(),
                            rscNameSuffix,
                            nameShortener.shorten(rscDfn, rscNameSuffix),
                            rscDataList,
                            nqn,
                            this,
                            transObjFactory,
                            transMgrProvider
                        );
                    }
                    catch (LinStorException lsExc)
                    {
                        throw new ImplementationError(
                            "Cannot reload Openflex resource definition from database",
                            lsExc
                        );
                    }
                    cacheRscDfnDataMap.put(
                        new Pair<>(
                            rscDfn,
                            rscNameSuffix
                        ),
                        new Pair<>(
                            ofRscDfnData,
                            rscDataList
                        )
                    );
                    rscDfn.setLayerData(dbCtx, ofRscDfnData);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (InvalidNameException exc)
        {
            throw new DatabaseException("Loaded invalid name from database ", exc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    private void fetchOfVlms(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef)
        throws DatabaseException
    {
        cachedVlmInfoMap = new HashMap<>();
        try (
            PreparedStatement loadAllVlms = getConnection().prepareStatement(SELECT_ALL_VLMS);
            ResultSet resultSet = loadAllVlms.executeQuery();
        )
        {
            while (resultSet.next())
            {
                int rscLayerId = resultSet.getInt(LAYER_RESOURCE_ID);
                int vlmNr = resultSet.getInt(VLM_NR);

                List<OpenflexVlmInfo> infoList = cachedVlmInfoMap.get(rscLayerId);
                if (infoList == null)
                {
                    infoList = new ArrayList<>();
                    cachedVlmInfoMap.put(rscLayerId, infoList);
                }

                NodeName nodeName = new NodeName(resultSet.getString(NODE_NAME));
                StorPoolName storPoolName = new StorPoolName(resultSet.getString(POOL_NAME));

                Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                    new Pair<>(nodeName, storPoolName)
                );
                infoList.add(
                    new OpenflexVlmInfo(
                        rscLayerId,
                        vlmNr,
                        storPoolWithInitMap.objA,
                        storPoolWithInitMap.objB
                    )
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (InvalidNameException exc)
        {
            throw new DatabaseException("Loaded invalid name from database ", exc);
        }
    }

    /**
     * Fully loads a {@link NvmeRscData} object including its {@link NvmeVlmData}
     * @param parentRef
     *
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<OpenflexRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, OpenflexVlmData<RSC>> vlmMap = new TreeMap<>();

        Pair<OpenflexRscDfnData<Resource>, ArrayList<OpenflexRscData<Resource>>> rscDfnDataPair = cacheRscDfnDataMap
            .get(
                new Pair<>(
                    absRsc.getResourceDefinition(),
                    rscSuffixRef
                )
            );

        OpenflexRscData<RSC> ofRscData = new OpenflexRscData<>(
            id,
            absRsc,
            (OpenflexRscDfnData<RSC>) rscDfnDataPair.objA, // FIXME as soon as snapshots are supported for openflex
            parentRef,
            children,
            vlmMap,
            this,
            transObjFactory,
            transMgrProvider
        );

        rscDfnDataPair.objB.add((OpenflexRscData<Resource>) ofRscData); // FIXME as soon as snapshots are supported for
                                                                        // openflex

        List<OpenflexVlmInfo> ofVlmInfoList = cachedVlmInfoMap.get(id);
        try
        {
            for (OpenflexVlmInfo ofVlmInfo : ofVlmInfoList)
            {
                VolumeNumber vlmNr = new VolumeNumber(ofVlmInfo.vlmNr);
                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);

                vlmMap.put(
                    vlmNr,
                    new OpenflexVlmData<>(vlm, ofRscData, ofVlmInfo.storPool, transObjFactory, transMgrProvider)
                );
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError("Invalid volume number loaded", exc);
        }
        return new Pair<>(ofRscData, children);
    }

    @Override
    public void create(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating OpenflexRscData %s", getId(ofRscDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_RSC_DFN))
        {
            stmt.setString(1, ofRscDfnData.getResourceName().value);
            stmt.setString(2, ofRscDfnData.getRscNameSuffix());
            if (ofRscDfnData.getNqn() == null)
            {
                stmt.setNull(3, Types.VARCHAR);
            }
            else
            {
                stmt.setString(3, ofRscDfnData.getNqn());
            }

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexRscDfnData %s", getId(ofRscDfnData));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_RSC_DFN))
        {
            stmt.setString(1, ofRscDfnData.getResourceName().value);
            stmt.setString(2, ofRscDfnData.getRscNameSuffix());

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void create(OpenflexRscData<?> ofRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if OpenflexRscData will get a database table in future.
    }

    @Override
    public void delete(OpenflexRscData<?> ofRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if OpenflexRscData will get a database table in future.
    }

    @Override
    public void persist(OpenflexVlmData<?> ofVlmData) throws DatabaseException
    {
        errorReporter.logTrace("Creating OpenflexVlmData %s", getId(ofVlmData));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, ofVlmData.getRscLayerId());
            stmt.setInt(2, ofVlmData.getVlmNr().value);
            StorPool storPool = ofVlmData.getStorPool();
            stmt.setString(3, storPool.getNode().getName().value);
            stmt.setString(4, storPool.getName().value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }


    @Override
    public void delete(OpenflexVlmData<?> ofVlmData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexVlmData %s", getId(ofVlmData));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, ofVlmData.getRscLayerId());
            stmt.setInt(2, ofVlmData.getVlmNr().value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    public static class OpenflexVlmInfo
    {
        public final int rscId;
        public final int vlmNr;
        public final StorPool storPool;
        public final StorPool.InitMaps storPoolInitMaps;

        public OpenflexVlmInfo(
            int rscIdRef,
            int vlmNrRef,
            StorPool storPoolRef,
            StorPool.InitMaps storPoolInitMapsRef
        )
        {
            rscId = rscIdRef;
            vlmNr = vlmNrRef;
            storPool = storPoolRef;
            storPoolInitMaps = storPoolInitMapsRef;
        }
    }

    private class NqnDriver implements SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String>
    {
        @Override
        public void update(OpenflexRscDfnData<?> ofRscDfnData, String nqn) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating OpenflexRscDfnData's NQN from [%s] to [%s] %s",
                ofRscDfnData.getNqn(),
                nqn,
                getId(ofRscDfnData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_NQN))
            {
                stmt.setString(1, nqn);

                stmt.setString(2, ofRscDfnData.getResourceName().value);
                stmt.setString(3, ofRscDfnData.getRscNameSuffix());

                stmt.executeUpdate();
            }
            catch (SQLException exc)
            {
                throw new DatabaseException(exc);
            }
        }
    }
}
