package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME_CACHE;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME_META;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_CACHE_VOLUMES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("checkstyle:magicnumber")
@Singleton
public class CacheLayerSQLDbDriver implements CacheLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        NODE_NAME,
        POOL_NAME_CACHE,
        POOL_NAME_META
    };

    private static final String SELECT_VLM_BY_RSC_ID =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_CACHE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_CACHE_VOLUMES +
        " (" + StringUtils.join(", ", VLM_ALL_FIELDS) + " ) " +
        " VALUES (" + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) + ")";
    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_CACHE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR +            " = ?";

    @Inject
    public CacheLayerSQLDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    /**
     * Fully loads a {@link NvmeRscData} object including its {@link NvmeVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws DatabaseException
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<CacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        NodeName nodeName = rsc.getNode().getName();

        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, CacheVlmData<RSC>> vlmMap = new TreeMap<>();
        CacheRscData<RSC> cacheRscData = new CacheRscData<>(
            id,
            rsc,
            parentRef,
            children,
            rscSuffixRef,
            this,
            vlmMap,
            transObjFactory,
            transMgrProvider
        );

        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_VLM_BY_RSC_ID))
        {
            stmt.setInt(1, id);

            int vlmNrInt = -1;
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    vlmNrInt = resultSet.getInt(VLM_NR);
                    String cacheStorPoolNameStr = resultSet.getString(POOL_NAME_CACHE);
                    String metaStorPoolNameStr = resultSet.getString(POOL_NAME_META);

                    VolumeNumber vlmNr;
                    vlmNr = new VolumeNumber(vlmNrInt);

                    AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
                    StorPool cachedStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            nodeName,
                            new StorPoolName(cacheStorPoolNameStr)
                        )
                    ).objA;
                    StorPool metaStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            nodeName,
                            new StorPoolName(metaStorPoolNameStr)
                        )
                    ).objA;

                    vlmMap.put(
                        vlm.getVolumeNumber(),
                        new CacheVlmData<>(
                            vlm,
                            cacheRscData,
                            cachedStorPool,
                            metaStorPool,
                            this,
                            transObjFactory,
                            transMgrProvider
                        )
                    );
                }
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored volume number " + vlmNrInt +
                        " for resource layer id: " + id
                );
            }
            catch (InvalidNameException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored storage pool name '" + exc.invalidName +
                        "' for resource layer id " + id + " vlmNr: " + vlmNrInt
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return new Pair<>(cacheRscData, children);
    }

    @Override
    public void persist(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if CacheRscData will get a database table in future.
    }

    @Override
    public void persist(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating CacheVlmData %s", getId(cacheVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, cacheVlmDataRef.getRscLayerId());
            stmt.setInt(2, cacheVlmDataRef.getVlmNr().value);

            StorPool cacheStorPool = cacheVlmDataRef.getCacheStorPool();
            StorPool metaStorPool = cacheVlmDataRef.getMetaStorPool();
            stmt.setString(3, cacheStorPool.getNode().getName().value);
            stmt.setString(4, cacheStorPool.getName().value);
            stmt.setString(5, metaStorPool.getName().value);

            stmt.executeUpdate();
            errorReporter.logTrace("CacheVlmData created %s", getId(cacheVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if CacheRscData will get a database table in future.
    }

    @Override
    public void delete(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting CacheVlmData %s", getId(cacheVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, cacheVlmDataRef.getRscLayerId());
            stmt.setInt(2, cacheVlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("CacheVlmData deleted %s", getId(cacheVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(CacheRscData<?> cacheRscData)
    {
        return "(LayerRscId=" + cacheRscData.getRscLayerId() +
            ", SuffResName=" + cacheRscData.getSuffixedResourceName() +
            ")";
    }

    private String getId(CacheVlmData<?> cacheVlmData)
    {
        return "(LayerRscId=" + cacheVlmData.getRscLayerId() +
            ", SuffResName=" + cacheVlmData.getRscLayerObject().getSuffixedResourceName() +
            ", VlmNr=" + cacheVlmData.getVlmNr().value +
            ")";
    }
}
