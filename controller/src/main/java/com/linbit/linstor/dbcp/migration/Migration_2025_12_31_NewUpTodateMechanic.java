package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.12.31.23.59",
    description = "Set existing VlmDfns as DRBD_INITIALIZED"
)
// technically this migration was added 2026.04.02, targeted in 1.34.0, but we needed an emergency-release
// but we also need to make sure to keep this (and k8s) migration in order. Sorry for the weird date
public class Migration_2025_12_31_NewUpTodateMechanic extends LinstorMigration
{
    public static final String PROP_KEY_NEW_LINSTOR_DRBD_INITIAL_UPTODATE_ON = "Linstor/Drbd/InitialUptoDateOn";
    public static final String PROP_KEY_OLD_DRBD_PRIMARY_SET_ON = "DrbdPrimarySetOn";
    private static final long VD_FLAG_DRBD_INIT = 1L << 5;

    private static final String SELECT_VLM_DFNS = "SELECT RESOURCE_NAME, SNAPSHOT_NAME, VLM_NR, VLM_FLAGS " +
        "FROM VOLUME_DEFINITIONS";
    private static final String SELECT_PROPS = "SELECT PROPS_INSTANCE, PROP_KEY, PROP_VALUE " +
        "FROM PROPS_CONTAINERS";

    private static final String INSERT_PROPS = "INSERT INTO PROPS_CONTAINERS " +
        "(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) " +
        "VALUES (?,?,?)";

    private static final String UPDATE_VLM_DFN_FLAGS = "UPDATE VOLUME_DEFINITIONS " +
        "SET VLM_FLAGS = ? " +
        "WHERE RESOURCE_NAME = ? AND SNAPSHOT_NAME = ? AND VLM_NR = ?";

    private static final String DEL_OLD_PROPS = "DELETE FROM PROPS_CONTAINERS " +
        "WHERE PROP_KEY = '" + PROP_KEY_OLD_DRBD_PRIMARY_SET_ON + "'";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        Map<VlmDfnKey, Long /* VlmDfnFlags */> vlmDfns = getVlmDfns(connection);
        Map<String, Map<String, String>> oldProps = getProps(connection);

        Map<VlmDfnKey, Result> migrationResult = getMigrationResult(vlmDfns, oldProps);

        apply(connection, migrationResult);
        deleteObsoleteRscDfnPrimaryOnProp(connection);
    }

    private static String buildVlmDfnPropsInstance(VlmDfnKey vlmDfnKeyRef)
    {
        String ret;
        if (vlmDfnKeyRef.snapName == null || vlmDfnKeyRef.snapName.isBlank())
        {
            ret = "/VLM_DFNS/" + vlmDfnKeyRef.rscName.toUpperCase() + "/" + vlmDfnKeyRef.vlmNr;
        }
        else
        {
            ret = "/SNAP_VLM_DFNS_VLM_DFN/" + vlmDfnKeyRef.rscName.toUpperCase() + "/" + vlmDfnKeyRef.snapName
                .toUpperCase() + "/" + vlmDfnKeyRef.vlmNr;
        }
        return ret;
    }

    private static String buildRscDfnPropsInstance(VlmDfnKey vlmDfnKeyRef)
    {
        String ret;
        if (vlmDfnKeyRef.snapName == null || vlmDfnKeyRef.snapName.isBlank())
        {
            ret = "/RSC_DFNS/" + vlmDfnKeyRef.rscName.toUpperCase();
        }
        else
        {
            ret = "/SNAP_DFNS_RSC_DFN/" + vlmDfnKeyRef.rscName.toUpperCase() + "/" + vlmDfnKeyRef.snapName
                .toUpperCase();
        }
        return ret;
    }

    public static Map<VlmDfnKey, Result> getMigrationResult(
        Map<VlmDfnKey, Long /* VlmDfnFlags*/> vlmDfnListRef,
        Map<String /* PropsInstance */, Map<String/* key */, String/* value */>> oldPropsRef
    )
    {
        Map<VlmDfnKey, Result> ret = new HashMap<>();
        for (Entry<VlmDfnKey, Long> vlmDfnEntry : vlmDfnListRef.entrySet())
        {
            VlmDfnKey vlmDfnKey = vlmDfnEntry.getKey();
            long vlmDfnFlags = vlmDfnEntry.getValue();
            @Nullable Map<String, String> rscDfnProps = oldPropsRef.get(buildRscDfnPropsInstance(vlmDfnKey));
            if (rscDfnProps != null)
            {
                @Nullable String rscDfnValue = rscDfnProps.get(PROP_KEY_OLD_DRBD_PRIMARY_SET_ON);
                if (rscDfnValue != null)
                {
                    final long updatedFlags = vlmDfnKey.snapName == null || vlmDfnKey.snapName.isBlank() ?
                        vlmDfnFlags | VD_FLAG_DRBD_INIT : // only set flags for VlmDfns, not for SnapVlmDfns
                        vlmDfnFlags;
                    ret.put(
                        vlmDfnKey,
                        new Result(
                            buildVlmDfnPropsInstance(vlmDfnKey),
                            rscDfnValue,
                            updatedFlags
                        )
                    );
                }
            }
        }
        return ret;
    }

    private Map<VlmDfnKey, Long/* VlmDfnFlags*/> getVlmDfns(Connection connection) throws SQLException
    {
        Map<VlmDfnKey, Long /* VlmDfnFlags*/> vlmDfns = new HashMap<>();
        try (
            PreparedStatement slctVlmDfns = connection.prepareStatement(SELECT_VLM_DFNS);
            ResultSet rs = slctVlmDfns.executeQuery();)
        {
            while (rs.next())
            {
                vlmDfns.put(
                    new VlmDfnKey(
                        rs.getString("RESOURCE_NAME"),
                        rs.getString("SNAPSHOT_NAME"),
                        rs.getInt("VLM_NR")
                    ),
                    rs.getLong("VLM_FLAGS")
                );
            }
        }
        return vlmDfns;
    }

    private Map<String, Map<String, String>> getProps(Connection connection) throws SQLException
    {
        Map<String /*PropsInstance*/, Map<String/*key*/, String/*value*/>> props = new HashMap<>();
        try (
            PreparedStatement slctProps = connection.prepareStatement(SELECT_PROPS);
            ResultSet rsProps = slctProps.executeQuery();)
        {
            while (rsProps.next())
            {
                props.computeIfAbsent(rsProps.getString("PROPS_INSTANCE"), ignored -> new HashMap<>())
                    .put(rsProps.getString("PROP_KEY"), rsProps.getString("PROP_VALUE"));
            }
        }
        return props;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void apply(Connection connection, Map<VlmDfnKey, Result> migrationResult) throws SQLException
    {
        try (
            PreparedStatement insertStmt = connection.prepareStatement(INSERT_PROPS);
            PreparedStatement updtVlmDfns = connection.prepareStatement(UPDATE_VLM_DFN_FLAGS))
        {
            insertStmt.setString(2, PROP_KEY_NEW_LINSTOR_DRBD_INITIAL_UPTODATE_ON);
            for (Map.Entry<VlmDfnKey, Result> entry : migrationResult.entrySet())
            {
                VlmDfnKey vlmDfnKey = entry.getKey();
                Result result = entry.getValue();
                insertStmt.setString(1, result.vlmDfnPropsInstance);
                insertStmt.setString(3, result.winningNodeNamePropValue);
                insertStmt.addBatch();

                updtVlmDfns.setString(2, vlmDfnKey.rscName);
                if (vlmDfnKey.snapName == null)
                {
                    updtVlmDfns.setNull(3, Types.VARCHAR);
                }
                else
                {
                    updtVlmDfns.setString(3, vlmDfnKey.snapName);
                }

                updtVlmDfns.setInt(4, vlmDfnKey.vlmNr);
                updtVlmDfns.setLong(1, result.updatedFlags);
                updtVlmDfns.addBatch();
            }
            insertStmt.executeBatch();
            updtVlmDfns.executeBatch();
        }
    }

    private void deleteObsoleteRscDfnPrimaryOnProp(Connection connectionRef) throws SQLException
    {
        try (PreparedStatement ps = connectionRef.prepareStatement(DEL_OLD_PROPS))
        {
            ps.execute();
        }
    }

    public static class VlmDfnKey
    {
        public final String rscName;
        public final @Nullable String snapName;
        public final int vlmNr;

        public VlmDfnKey(String rscNameRef, @Nullable String snapNameRef, int vlmNrRef)
        {
            rscName = rscNameRef;
            snapName = snapNameRef;
            vlmNr = vlmNrRef;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, snapName, vlmNr);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (!(obj instanceof VlmDfnKey other))
            {
                return false;
            }
            return Objects.equals(rscName, other.rscName) && Objects.equals(snapName, other.snapName) &&
                vlmNr == other.vlmNr;
        }

        @Override
        public String toString()
        {
            return "VlmDfnKey [rscName=" + rscName + ", snapName=" + snapName + ", vlmNr=" + vlmNr + "]";
        }
    }

    // could have used a record, but this still needs to be compatible with java 11
    public static class Result
    {
        public final String vlmDfnPropsInstance;
        public final String winningNodeNamePropValue;
        public final long updatedFlags;

        public Result(String vlmDfnPropsInstanceRef, String winningNodeNamePropValueRef, long updatedFlagsRef)
        {
            vlmDfnPropsInstance = vlmDfnPropsInstanceRef;
            winningNodeNamePropValue = winningNodeNamePropValueRef;
            updatedFlags = updatedFlagsRef;
        }
    }
}
