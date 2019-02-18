package com.linbit.linstor;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.ENCRYPTED_PASSWORD;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.LAYER_RESOURCE_ID;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_LAYER_CRYPT_SETUP_VOLUMES;
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

public class CryptSetupLayerGenericDbDriver implements CryptSetupLayerDatabaseDriver
{
    // no special table for resource-data
    private static final String VLM_ALL_FIELDS =
        LAYER_RESOURCE_ID + ", " + VLM_NR + ", " + ENCRYPTED_PASSWORD;

    private static final String SELECT_ALL_VLMS_BY_RSC_ID =
        " SELECT " + VLM_ALL_FIELDS +
        " FROM " + TBL_LAYER_CRYPT_SETUP_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ?";

    private static final String INSERT_VLM =
        " INSERT INTO " + TBL_LAYER_CRYPT_SETUP_VOLUMES +
        " ( " + VLM_ALL_FIELDS + " ) " +
        " VALUES ( " + VLM_ALL_FIELDS.replaceAll("[^, ]+", "?") + " )";

    private static final String DELETE_VLM =
        " DELETE FROM " + TBL_LAYER_CRYPT_SETUP_VOLUMES +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ? ";

    private static final String UPDATE_VLM_PW =
        " UPDATE " + TBL_LAYER_CRYPT_SETUP_VOLUMES +
        " SET " + ENCRYPTED_PASSWORD + " = ? " +
        " WHERE " + LAYER_RESOURCE_ID + " = ? AND " +
                    VLM_NR            + " = ? ";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    private final VlmPwDriver vlmPwDriver;

    @Inject
    public CryptSetupLayerGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmPwDriver = new VlmPwDriver();
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
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public Pair<CryptSetupRscData, Set<RscLayerObject>> load(
        Resource rsc,
        int id,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws SQLException
    {
        Pair<DrbdRscData, Set<RscLayerObject>> ret;
        Set<RscLayerObject> childrenRscDataList = new HashSet<>();
        Map<VolumeNumber, CryptSetupVlmData> vlmDataMap = new TreeMap<>();
        CryptSetupRscData rscData = new CryptSetupRscData(
            id,
            rsc,
            rscSuffixRef,
            parentRef,
            childrenRscDataList,
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
                        throw new LinStorSqlRuntimeException(
                            "Failed to restore stored volume number " + resultSet.getInt(2)
                        );
                    }
                    vlmDataMap.put(
                        vlmNr,
                        new CryptSetupVlmData(
                            rsc.getVolume(vlmNr),
                            rscData,
                            resultSet.getString(ENCRYPTED_PASSWORD).getBytes(),
                            this,
                            transObjFactory,
                            transMgrProvider
                        )
                    );
                }
            }
        }

        return new Pair<>(rscData, childrenRscDataList);
    }

    @Override
    public void persist(CryptSetupRscData cryptRscDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if CryptSetupRscData will get a database table in future.
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public void persist(CryptSetupVlmData cryptVlmDataRef) throws SQLException
    {
        errorReporter.logTrace("Creating CryptSetupVlmData %s", getId(cryptVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(INSERT_VLM))
        {
            stmt.setInt(1, cryptVlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, cryptVlmDataRef.getVlmNr().value);
            stmt.setBytes(3, cryptVlmDataRef.getEncryptedPassword());

            stmt.executeUpdate();
            errorReporter.logTrace("CryptSetupVlmData created %s", getId(cryptVlmDataRef));
        }
    }

    @Override
    public void delete(CryptSetupRscData cryptRscDataRef) throws SQLException
    {
        // no-op - there is no special database table.
        // this method only exists if DrbdVlmData will get a database table in future.
    }

    @Override
    public void delete(CryptSetupVlmData cryptVlmDataRef) throws SQLException
    {
        errorReporter.logTrace("Deleting CryptSetupVlmData %s", getId(cryptVlmDataRef));
        try (PreparedStatement stmt = getConnection().prepareStatement(DELETE_VLM))
        {
            stmt.setInt(1, cryptVlmDataRef.getRscLayerObject().getRscLayerId());
            stmt.setInt(2, cryptVlmDataRef.getVlmNr().value);

            stmt.executeUpdate();
            errorReporter.logTrace("CryptSetupVlmData deleted %s", getId(cryptVlmDataRef));
        }
    }

    @Override
    public SingleColumnDatabaseDriver<CryptSetupVlmData, byte[]> getVlmEncryptedPasswordDriver()
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

    private String getId(CryptSetupVlmData cryptVlmDataRef)
    {
        return "(LayerRscId=" + cryptVlmDataRef.getRscLayerId() +
            ", VlmNr=" + cryptVlmDataRef.getVlmNr().value +
            ")";
    }

    private class VlmPwDriver implements SingleColumnDatabaseDriver<CryptSetupVlmData, byte[]>
    {
        @SuppressWarnings("checkstyle:magicnumber")
        @Override
        public void update(CryptSetupVlmData cryptVlmDataRef, byte[] encryptedPassword)
            throws SQLException
        {
            errorReporter.logTrace(
                "Updating CryptSetupVlmData's encrypted password %s",
                getId(cryptVlmDataRef)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(UPDATE_VLM_PW))
            {
                stmt.setBytes(1, encryptedPassword);
                stmt.setInt(2, cryptVlmDataRef.getRscLayerObject().getRscLayerId());
                stmt.setInt(3, cryptVlmDataRef.getVlmNr().value);

                stmt.executeUpdate();
            }
            errorReporter.logTrace(
                "CryptSetupVlmData's secret updated %s",
                getId(cryptVlmDataRef)
            );
        }
    }
}
