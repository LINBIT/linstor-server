package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DEV_UUID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.NODE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.POOL_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_BCACHE_VOLUMES;
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
import java.util.UUID;

@Singleton
public class BCacheLayerSQLDbDriver implements BCacheLayerCtrlDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private static final String[] VLM_ALL_FIELDS =
    {
        LAYER_RESOURCE_ID,
        VLM_NR,
        NODE_NAME,
        POOL_NAME
    };

    private static final String SELECT_VLM_BY_RSC_ID =
        " SELECT " + StringUtils.join(", ", VLM_ALL_FIELDS) +
        " FROM " + TBL_LAYER_BCACHE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_BCACHE_VOLUMES +
        " (" + StringUtils.join(", ", VLM_ALL_FIELDS) + " ) " +
        " VALUES (" + StringUtils.repeat("?", ", ", VLM_ALL_FIELDS.length) +")";
    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_BCACHE_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR +            " = ?";
    public static final String UPDATE_VLM_DEVICE_UUID =
        " UPDATE " + TBL_LAYER_BCACHE_VOLUMES +
        " SET " + DEV_UUID + " = ?, " +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ?";

    private final VlmDeviceUuidDriver vlmDeviceUuidDriver;

    @Inject
    public BCacheLayerSQLDbDriver(
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmDeviceUuidDriver = new VlmDeviceUuidDriver();
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
    public <RSC extends AbsResource<RSC>> Pair<BCacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
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
        Map<VolumeNumber, BCacheVlmData<RSC>> vlmMap = new TreeMap<>();
        BCacheRscData<RSC> bcacheRscData = new BCacheRscData<>(
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
                    String cacheStorPoolNameStr = resultSet.getString(POOL_NAME);

                    VolumeNumber vlmNr;
                    vlmNr = new VolumeNumber(vlmNrInt);

                    AbsVolume<RSC> vlm = rsc.getVolume(vlmNr);
                    StorPool cachedStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            nodeName,
                            new StorPoolName(cacheStorPoolNameStr)
                        )
                    ).objA;

                    vlmMap.put(
                        vlm.getVolumeNumber(),
                        new BCacheVlmData<>(
                            vlm,
                            bcacheRscData,
                            cachedStorPool,
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
        return new Pair<>(bcacheRscData, children);
    }

    @Override
    public void persist(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if BCacheRscData will get a database table in future.
    }

    @Override
    public void persist(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating BCacheVlmData %s", getId(bcacheVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, bcacheVlmDataRef.getRscLayerId());
            stmt.setInt(2, bcacheVlmDataRef.getVlmNr().value);

            StorPool cacheStorPool = bcacheVlmDataRef.getCacheStorPool();
            stmt.setString(3, cacheStorPool.getNode().getName().value);
            stmt.setString(4, cacheStorPool.getName().value);

            stmt.executeUpdate();
            errorReporter.logTrace("BCacheVlmData created %s", getId(bcacheVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if BCacheRscData will get a database table in future.
    }

    @Override
    public void delete(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting BCacheVlmData %s", getId(bcacheVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, bcacheVlmDataRef.getRscLayerId());
            stmt.setInt(2, bcacheVlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("BCacheVlmData deleted %s", getId(bcacheVlmDataRef));
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

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return vlmDeviceUuidDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(BCacheVlmData<?> bcacheVlmData)
    {
        return "(LayerRscId=" + bcacheVlmData.getRscLayerId() +
            ", SuffResName=" + bcacheVlmData.getRscLayerObject().getSuffixedResourceName() +
            ", VlmNr=" + bcacheVlmData.getVlmNr().value +
            ")";
    }

    private class VlmDeviceUuidDriver implements SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID>
    {
        @Override
        public void update(BCacheVlmData<?> bcacheVlmData, UUID uuid) throws DatabaseException
        {
            String fromStr = null;
            String toStr = uuid.toString();
            if (bcacheVlmData.getDeviceUuid() != null)
            {
                fromStr = bcacheVlmData.getDeviceUuid().toString();
            }
            errorReporter.logTrace(
                "Updating BCacheVlmData's UUID from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(bcacheVlmData)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_VLM_DEVICE_UUID))
            {
                stmt.setString(1, uuid.toString());
                stmt.setLong(2, bcacheVlmData.getRscLayerId());
                stmt.setInt(3, bcacheVlmData.getVlmNr().value);

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
                getId(bcacheVlmData)
            );
        }
    }
}
