package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.10.28.08.00",
    description = "Disable auto-block-size for existing resources"
)
public class Migration_2025_10_28_DisableAutoBlockSizesForExistingResources extends LinstorMigration
{
    public static final String TBL_RSC_DFN = "LAYER_RESOURCE_IDS";
    public static final String TBL_PROPS = "PROPS_CONTAINERS";

    public static final String CLM_RD_RSC_NAME = "RESOURCE_NAME";

    public static final String CLM_PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String CLM_PROPS_KEY = "PROP_KEY";
    public static final String CLM_PROPS_VALUE = "PROP_VALUE";

    private static final String PROP_KEY_AUTO_BLK_SIZE = "Linstor/Drbd/auto-block-size";
    private static final String PROP_VALUE_FALSE = "False";

    private static final String SELECT_ALL_RD_NAMES = "SELECT " + CLM_RD_RSC_NAME + " FROM " + TBL_RSC_DFN;

    private static final String INSERT_PROP = "INSERT INTO " + TBL_PROPS + "(" + CLM_PROPS_INSTANCE + ", " +
        CLM_PROPS_KEY + ", " + CLM_PROPS_VALUE + ") VALUES (?, ?, ?)";
    private static final int INSERT_PROP_INST_IDX = 1;
    private static final int INSERT_PROP_KEY_IDX = 2;
    private static final int INSERT_PROP_VALUE_IDX = 3;

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        Collection<String> rscNames = getRscDfnNames(conRef);
        HashMap<String, HashMap<String, String>> propsToCreate = getPropsToInsert(rscNames);
        insertProps(conRef, propsToCreate);
    }

    private Collection<String> getRscDfnNames(Connection conRef) throws SQLException
    {
        HashSet<String> ret = new HashSet<>();
        try (
            PreparedStatement prepStmt = conRef.prepareStatement(SELECT_ALL_RD_NAMES);
            ResultSet selectResultSet = prepStmt.executeQuery();)
        {
            while (selectResultSet.next())
            {
                ret.add(selectResultSet.getString(CLM_RD_RSC_NAME));
            }
        }

        return ret;
    }

    private void insertProps(Connection conRef, HashMap<String, HashMap<String, String>> propsToCreateRef)
        throws SQLException
    {
        try (PreparedStatement insertStmt = conRef.prepareStatement(INSERT_PROP))
        {
            for (Map.Entry<String, HashMap<String, String>> outerEntry : propsToCreateRef.entrySet())
            {
                String instanceName = outerEntry.getKey();
                insertStmt.setString(INSERT_PROP_INST_IDX, instanceName);
                for (Map.Entry<String, String> innerEntry : outerEntry.getValue().entrySet())
                {
                    String key = innerEntry.getKey();
                    String value = innerEntry.getValue();
                    insertStmt.setString(INSERT_PROP_KEY_IDX, key);
                    insertStmt.setString(INSERT_PROP_VALUE_IDX, value);
                    insertStmt.addBatch();
                }
            }
            insertStmt.executeBatch();
        }
    }

    public static HashMap<String, HashMap<String, String>> getPropsToInsert(
        Collection<String> rscNamesRef
    )
    {
        HashMap<String, HashMap<String, String>> ret = new HashMap<>();
        for (String rscName : rscNamesRef)
        {
            // disable auto-block-size by default for all existing resources
            final String instanceName = getInstanceName(rscName);
            ret.computeIfAbsent(instanceName, ignored -> new HashMap<>())
                .put(PROP_KEY_AUTO_BLK_SIZE, PROP_VALUE_FALSE);
        }
        return ret;
    }

    private static String getInstanceName(String rscNameRef)
    {
        return "/RSC_DFNS/" + rscNameRef.toUpperCase();
    }

    public static class LriInfo
    {
        public final int id;
        public final String rscName;
        public final String kind;
        public final @Nullable Integer parentId;

        public LriInfo(
            int idRef,
            String rscNameRef,
            String kindRef,
            @Nullable Integer parentIdRef
        )
        {
            id = idRef;
            rscName = rscNameRef;
            kind = kindRef;
            parentId = parentIdRef;
        }
    }

    public static class StorVlmInfo
    {
        public final int lriId;
        public final int vlmNr;
        public final String providerKind;

        public StorVlmInfo(int lriIdRef, int vlmNrRef, String providerKindRef)
        {
            lriId = lriIdRef;
            vlmNr = vlmNrRef;
            providerKind = providerKindRef;
        }
    }
}
