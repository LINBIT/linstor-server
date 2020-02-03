package com.linbit.linstor.core.objects;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.ENCRYPTED_PASSWORD;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_LUKS_VOLUMES;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class LuksLayerSQLDbDriver implements LuksLayerCtrlDatabaseDriver
{
    // no special table for resource-data
    private static final String VLM_ALL_FIELDS =
        LAYER_RESOURCE_ID + ", " + VLM_NR + ", " + ENCRYPTED_PASSWORD;

    private static final String SELECT_ALL_VLMS_BY_RSC_ID =
        " SELECT " + VLM_ALL_FIELDS +
        " FROM " + TBL_LAYER_LUKS_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_LUKS_VOLUMES +
        " ( " + VLM_ALL_FIELDS + " ) " +
        " VALUES ( " + VLM_ALL_FIELDS.replaceAll("[^, ]+", "?") + " )";

    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_LUKS_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ? ";

    private static final String UPDATE_VLM_PW =
        " UPDATE " + TBL_LAYER_LUKS_VOLUMES +
        " SET " + ENCRYPTED_PASSWORD + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ? ";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final VlmPwDriver vlmPwDriver;

    @Inject
    public LuksLayerSQLDbDriver(
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

        vlmPwDriver = new VlmPwDriver();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public <RSC extends AbsResource<RSC>> Pair<LuksRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws DatabaseException
    {
        Pair<LuksRscData<RSC>, Set<AbsRscLayerObject<RSC>>> ret;
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, LuksVlmData<RSC>> vlmDataMap = new TreeMap<>();

        LuksRscData<RSC> rscData = new LuksRscData<>(
            id,
            absRsc,
            rscSuffixRef,
            parentRef,
            children,
            vlmDataMap,
            this,
            transObjFactory,
            transMgrProvider
        );

        // load volumes
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_VLMS_BY_RSC_ID))
        {
            stmt.setInt(1, id);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    VolumeNumber vlmNr;
                    try
                    {
                        vlmNr = new VolumeNumber(resultSet.getInt(VLM_NR));
                    }
                    catch (ValueOutOfRangeException exc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Failed to restore stored volume number " + resultSet.getInt(2)
                        );
                    }
                    vlmDataMap.put(
                        vlmNr,
                        new LuksVlmData<>(
                            absRsc.getVolume(vlmNr),
                            rscData,
                            resultSet.getBytes(ENCRYPTED_PASSWORD),
                            this,
                            transObjFactory,
                            transMgrProvider
                        )
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        return new Pair<>(rscData, children);
    }

    @Override
    public void persist(LuksRscData<?> luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public void persist(LuksVlmData<?> luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating LuksVlmData %s", getId(luksVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, luksVlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, luksVlmDataRef.getVlmNr().value);
            stmt.setBytes(3, luksVlmDataRef.getEncryptedKey());

            stmt.executeUpdate();
            errorReporter.logTrace("LuksVlmData created %s", getId(luksVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void delete(LuksRscData<?> luksRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if LuksRscData will get a database table in future.
    }

    @Override
    public void delete(LuksVlmData<?> luksVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting LuksVlmData %s", getId(luksVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, luksVlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, luksVlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("LuksVlmData deleted %s", getId(luksVlmDataRef));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver()
    {
        return vlmPwDriver;
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

    private String getId(LuksVlmData<?> luksVlmDataRef)
    {
        return "(LayerRscId=" + luksVlmDataRef.getRscLayerId() +
            ", VlmNr=" + luksVlmDataRef.getVlmNr().value +
            ")";
    }

    private class VlmPwDriver implements SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]>
    {
        @SuppressWarnings("checkstyle:magicnumber")
        @Override
        public void update(LuksVlmData<?> luksVlmDataRef, byte[] encryptedPassword)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating LuksVlmData's encrypted password %s",
                getId(luksVlmDataRef)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_VLM_PW))
            {
                stmt.setBytes(1, encryptedPassword);
                stmt.setInt(2, luksVlmDataRef.getRscLayerObject().getRscLayerId());
                stmt.setInt(3, luksVlmDataRef.getVlmNr().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "LuksVlmData's secret updated %s",
                getId(luksVlmDataRef)
            );
        }
    }
}
