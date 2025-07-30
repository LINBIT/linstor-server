package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.regex.Pattern;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2024.12.18.10.00",
    description = "Split snapshotted resource properties - Fix more properties"
)
public class Migration_2024_12_18_SplitSnapPropsFixes extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";

    private static final String PROPS_INSTANCE_SNAP_DFNS_RSC_DFN = "/SNAP_DFNS_RSC_DFN/";
    private static final String PROPS_INSTANCE_SNAP_DFNS = "/SNAP_DFNS/";
    private static final int PROPS_INSTANCE_SNAP_DFNS_RSC_DFN_LEN = PROPS_INSTANCE_SNAP_DFNS_RSC_DFN.length();
    private static final HashSet<Pattern> SNAP_DFN_KEYS_TO_MOVE = new HashSet<>();

    private static final String PROPS_INSTANCE_SNAP_VLM_DFNS_VLM_DFN = "/SNAP_VLM_DFNS_VLM_DFN/";
    private static final String PROPS_INSTANCE_SNAP_VLM_DFNS = "/SNAP_VLM_DFNS/";
    private static final int PROPS_INSTANCE_SNAP_VLM_DFNS_VLM_DFN_LEN = PROPS_INSTANCE_SNAP_VLM_DFNS_VLM_DFN.length();
    private static final HashSet<Pattern> SNAP_VLM_DFN_KEYS_TO_MOVE = new HashSet<>();

    private static final String PROPS_INSTANCE_SNAP_VLMS_VLM = "/SNAP_VLMS_VLM/";
    private static final String PROPS_INSTANCE_SNAP_VLMS = "/SNAP_VLMS/";
    private static final int PROPS_INSTANCE_SNAP_VLM_VLM_LEN = PROPS_INSTANCE_SNAP_VLMS_VLM.length();
    private static final HashSet<Pattern> SNAP_VLM_KEYS_TO_MOVE = new HashSet<>();

    private static final String SELECT_STMT = "SELECT " + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS;

    private static final String UPDATE_STMT = "UPDATE " + TBL_PROPS_CONTAINERS + " SET " + PROPS_INSTANCE + " = ? " +
        "WHERE " + PROPS_INSTANCE + " = ? AND " + PROP_KEY + " = ? ";
    private static final int UPDATE_STMT_SET_PROPS_INSTANCE_ID = 1;
    private static final int UPDATE_STMT_QUERY_PROPS_INSTANCE_ID = 2;
    private static final int UPDATE_STMT_QUERY_PROPS_KEY_ID = 3;

    static
    {
        SNAP_DFN_KEYS_TO_MOVE.add(Pattern.compile("^SequenceNumber$"));
        SNAP_DFN_KEYS_TO_MOVE.add(Pattern.compile("^Backup/SourceSnapDfnUUID$"));
        SNAP_DFN_KEYS_TO_MOVE.add(Pattern.compile("^SnapshotShippingNamePrev$"));
        SNAP_DFN_KEYS_TO_MOVE.add(Pattern.compile("^Shipping/.*"));
        SNAP_DFN_KEYS_TO_MOVE.add(Pattern.compile("^Schedule/BackupStartTimestamp$"));

        SNAP_VLM_DFN_KEYS_TO_MOVE.add(Pattern.compile("^Shipping/.*"));

        SNAP_VLM_KEYS_TO_MOVE.add(Pattern.compile("^Satellite/EBS/EbsSnapId.*"));
    }

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectStmt = conRef.prepareStatement(SELECT_STMT);
            PreparedStatement updStmt = conRef.prepareStatement(UPDATE_STMT);
        )
        {
            ResultSet resultSet = selectStmt.executeQuery();
            while (resultSet.next())
            {
                String origInstanceName = resultSet.getString(PROPS_INSTANCE);
                String key = resultSet.getString(PROP_KEY);

                String newInstanceName = getNewInstanceName(origInstanceName, key);

                if (!origInstanceName.equals(newInstanceName))
                {
                    updStmt.setString(UPDATE_STMT_SET_PROPS_INSTANCE_ID, newInstanceName);
                    updStmt.setString(UPDATE_STMT_QUERY_PROPS_INSTANCE_ID, origInstanceName);
                    updStmt.setString(UPDATE_STMT_QUERY_PROPS_KEY_ID, key);

                    updStmt.executeUpdate();
                }
            }
        }
    }

    public static String getNewInstanceName(String origInstanceNameRef, String keyRef)
    {
        String newInstanceName;
        @Nullable HashSet<Pattern> patternsToCheck = null;
        @Nullable String newInstanceNamePrefix = null;
        @Nullable Integer oldInstanceNameOffset = null;

        if (origInstanceNameRef.startsWith(PROPS_INSTANCE_SNAP_DFNS_RSC_DFN))
        {
            patternsToCheck = SNAP_DFN_KEYS_TO_MOVE;
            newInstanceNamePrefix = PROPS_INSTANCE_SNAP_DFNS;
            oldInstanceNameOffset = PROPS_INSTANCE_SNAP_DFNS_RSC_DFN_LEN;
        }
        else if (origInstanceNameRef.startsWith(PROPS_INSTANCE_SNAP_VLM_DFNS_VLM_DFN))
        {
            patternsToCheck = SNAP_VLM_DFN_KEYS_TO_MOVE;
            newInstanceNamePrefix = PROPS_INSTANCE_SNAP_VLM_DFNS;
            oldInstanceNameOffset = PROPS_INSTANCE_SNAP_VLM_DFNS_VLM_DFN_LEN;
        }
        else if (origInstanceNameRef.startsWith(PROPS_INSTANCE_SNAP_VLMS_VLM))
        {
            patternsToCheck = SNAP_VLM_KEYS_TO_MOVE;
            newInstanceNamePrefix = PROPS_INSTANCE_SNAP_VLMS;
            oldInstanceNameOffset = PROPS_INSTANCE_SNAP_VLM_VLM_LEN;
        }
        if (patternsToCheck != null && newInstanceNamePrefix != null && oldInstanceNameOffset != null)
        {
            newInstanceName = check(
                keyRef,
                patternsToCheck,
                origInstanceNameRef,
                newInstanceNamePrefix,
                oldInstanceNameOffset
            );
        }
        else
        {
            newInstanceName = origInstanceNameRef;
        }
        return newInstanceName;
    }

    private static String check(
        String keyRef,
        HashSet<Pattern> patternsToCheckReff,
        String origInstanceNameRef,
        String newInstanceNamePrefixRef,
        int oldInstanceNameOffsetRef
    )
    {
        @Nullable String newInstanceName = null;
        for (Pattern pattern : patternsToCheckReff)
        {
            if (pattern.matcher(keyRef).matches())
            {
                newInstanceName = newInstanceNamePrefixRef + origInstanceNameRef.substring(oldInstanceNameOffsetRef);
            }
        }
        if (newInstanceName == null)
        {
            newInstanceName = origInstanceNameRef;
        }
        return newInstanceName;
    }
}
