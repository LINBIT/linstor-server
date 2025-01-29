package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.utils.StringUtils;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.01.29.10.00",
    description = "Split snapshotted resource properties - Fix more properties (2)"
)
public class Migration_2025_01_29_SplitSnapPropsMoreFixes extends LinstorMigration
{
    public static final String INSTANCE_NAME_RSC_GRP_PREFIX_OLD = "/RESOURCEGROUPS/";
    public static final String INSTANCE_NAME_RSC_GRP_PREFIX_NEW = "/RSC_GRPS/";

    public static final String INSTANCE_NAME_VLM_CON_PREFIX_OLD = "/CONDFN/VOLUME/";
    public static final String INSTANCE_NAME_VLM_CON_PREFIX_NEW = "/VLM_CONNS/";

    public static final String INSTANCE_NAME_SNAPS_RSC_PREFIX_OLD = "/SNAP_RSCS/";
    public static final String INSTANCE_NAME_SNAPS_RSC_PREFIX_NEW = "/SNAPS_RSC/";

    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";


    private static final String SELECT_STMT = "SELECT " + StringUtils.join(", ", PROPS_INSTANCE, PROP_KEY, PROP_VALUE) +
        " FROM " + TBL_PROPS_CONTAINERS;

    private static final String UPDATE_INSTANCE_NAME_STMT = "UPDATE " + TBL_PROPS_CONTAINERS +
        " SET " + PROPS_INSTANCE + " = ? " +
        " WHERE " + PROPS_INSTANCE + " = ? AND " + PROP_KEY + " = ?";
    private static final int UPDATE_INSTANCE_NAME_NEW_PROPS_INSTANCE_ID = 1;
    private static final int UPDATE_INSTANCE_NAME_OLD_PROPS_INSTANCE_ID = 2;
    private static final int UPDATE_INSTANCE_NAME_PROP_KEY_ID = 3;

    private static final String UPDATE_VALUE_STMT = "UPDATE " + TBL_PROPS_CONTAINERS +
        " SET " + PROP_VALUE + " = ? " +
        " WHERE " + PROPS_INSTANCE + " = ? AND " + PROP_KEY + " = ?";
    private static final int UPDATE_VALUE_PROP_VALUE_ID = 1;
    private static final int UPDATE_VALUE_NEW_PROPS_INSTANCE_ID = 2;
    private static final int UPDATE_VALUE_PROP_KEY_ID = 3;

    private static final String DELETE_NEW_PROPS_INSTANCE = "DELETE FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ?";
    private static final int DELETE_NEW_INSTANCE_NAME_PROPS_INSTANCE_ID = 1;

    @Override
    protected void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectStmt = conRef.prepareStatement(SELECT_STMT);
            PreparedStatement updInstanceNameStmt = conRef.prepareStatement(UPDATE_INSTANCE_NAME_STMT);
            PreparedStatement updValueStmt = conRef.prepareStatement(UPDATE_VALUE_STMT);
            PreparedStatement delNewInstanceNameStmt = conRef.prepareStatement(DELETE_NEW_PROPS_INSTANCE);
        )
        {
            ResultSet resultSet = selectStmt.executeQuery();
            Map<String/* instanceName */, Map<String/* propKey */, String/* propValue */>> allProps = new HashMap<>();
            while (resultSet.next())
            {
                String origInstanceName = resultSet.getString(PROPS_INSTANCE);
                String key = resultSet.getString(PROP_KEY);
                String value = resultSet.getString(PROP_VALUE);

                allProps.computeIfAbsent(origInstanceName, ignore -> new HashMap<>())
                    .put(key, value);
            }

            Changes changes = calculateChanges(allProps);

            executeOverrideInstanceNames(updInstanceNameStmt, changes.entriesToOverrideInstanceName);
            executeUpdateValues(updValueStmt, delNewInstanceNameStmt, changes.entriesToUpdateValues);
        }
    }

    private void executeOverrideInstanceNames(
        PreparedStatement updInstanceNameStmtRef,
        Map<String, Set<String>> entriesToOverrideInstanceNameRef
    )
        throws DatabaseException, SQLException
    {
        for (Entry<String, Set<String>> entryToOverrideInstanceName : entriesToOverrideInstanceNameRef.entrySet())
        {
            String oldInstanceName = entryToOverrideInstanceName.getKey();
            String newInstanceName = getNewInstanceName(oldInstanceName);

            updInstanceNameStmtRef.setString(UPDATE_INSTANCE_NAME_NEW_PROPS_INSTANCE_ID, newInstanceName);
            updInstanceNameStmtRef.setString(UPDATE_INSTANCE_NAME_OLD_PROPS_INSTANCE_ID, oldInstanceName);
            for (String key : entryToOverrideInstanceName.getValue())
            {
                updInstanceNameStmtRef.setString(UPDATE_INSTANCE_NAME_PROP_KEY_ID, key);
                updInstanceNameStmtRef.addBatch();
            }
        }
        executeBatch(updInstanceNameStmtRef);
    }

    private void executeUpdateValues(
        PreparedStatement updValueStmtRef,
        PreparedStatement delNewInstanceNameStmtRef,
        Map<String, Map<String, String>> entriesToUpdateValuesRef
    )
        throws SQLException, DatabaseException
    {
        for (Entry<String, Map<String, String>> entry : entriesToUpdateValuesRef.entrySet())
        {
            String oldInstanceName = entry.getKey();
            Map<String, String> propsWithNewValues = entry.getValue();

            String newInstanceName = getNewInstanceName(oldInstanceName);
            updValueStmtRef.setString(UPDATE_VALUE_NEW_PROPS_INSTANCE_ID, newInstanceName);
            for (Entry<String, String> propsEntry : propsWithNewValues.entrySet())
            {
                updValueStmtRef.setString(UPDATE_VALUE_PROP_KEY_ID, propsEntry.getKey());
                updValueStmtRef.setString(UPDATE_VALUE_PROP_VALUE_ID, propsEntry.getValue());
                updValueStmtRef.addBatch();
            }
            delNewInstanceNameStmtRef.setString(DELETE_NEW_INSTANCE_NAME_PROPS_INSTANCE_ID, oldInstanceName);
            delNewInstanceNameStmtRef.addBatch();
        }
        executeBatch(updValueStmtRef);
        executeBatch(delNewInstanceNameStmtRef);
    }

    private void executeBatch(PreparedStatement updStmtRef) throws SQLException, DatabaseException
    {
        int[] results = updStmtRef.executeBatch();
        for (int result : results)
        {
            if (result == Statement.EXECUTE_FAILED)
            {
                throw new DatabaseException("Some updates failed");
            }
        }
    }

    public static Changes calculateChanges(Map<String, Map<String, String>> allPropsRef) throws DatabaseException
    {
        Changes ret = new Changes();

        for (Entry<String, Map<String, String>> entry : allPropsRef.entrySet())
        {
            String instanceName = entry.getKey();
            if (instanceName.startsWith(INSTANCE_NAME_RSC_GRP_PREFIX_OLD))
            {
                fixRscGrpProps(allPropsRef, entry, ret);
            }
            else if (instanceName.startsWith(INSTANCE_NAME_VLM_CON_PREFIX_OLD))
            {
                fixVlmConProps(entry, ret);
            }
            else if (instanceName.startsWith(INSTANCE_NAME_SNAPS_RSC_PREFIX_OLD))
            {
                fixSnapsRscProps(entry, ret);
            }
        }
        return ret;
    }

    private static void fixRscGrpProps(
        Map<String, Map<String, String>> allPropsRef,
        Entry<String, Map<String, String>> entryRef,
        Changes changesRef
    )
        throws DatabaseException
    {
        Map<String, Set<String>> entriesToOverrideInstanceName = changesRef.entriesToOverrideInstanceName;
        Map<String, Map<String, String>> entriesToUpdateValues = changesRef.entriesToUpdateValues;

        // instanceName is guaranteed to start with "/RESOURCEGROUPS/", which means the current rscGrp's
        // properties were created after the migration where we changed "/RESOURCEGROUPS/" -> "/RSC_GRPS/"
        String instanceName = entryRef.getKey();

        // since instanceName is the pre-migration version (i.e. "/RESOURCEGROUPS/"), all properties in this
        // instance name were added post-migration of "/RESOURCEGROUPS/" -> "/RSC_GRPS/" and are therefore the
        // new properties
        Map<String, String> newProps = entryRef.getValue();

        // newInstanceName starts with "/RSC_GRPS/"
        String newInstanceName = getNewInstanceName(instanceName);
        // since newInstanceName is the post-migration version (i.e. "/RSC_GRPS/"), all properties of
        // newInstanceNames were added pre-migration of "/RESOURCEGROUPS/" -> "/RSC_GRPS/" and are therefore the
        // old properties
        @Nullable Map<String, String> oldProps = allPropsRef.get(newInstanceName);

        if (oldProps == null)
        {
            // if we do not have any old properties, we can safely move all properties of instanceName into
            // newInstanceName
            entriesToOverrideInstanceName.put(instanceName, newProps.keySet());
        }
        else
        {
            // we have both, new properties as well as old properties.

            for (Entry<String, String> propEntry : newProps.entrySet())
            {
                String key = propEntry.getKey();

                @Nullable String oldValue = oldProps.get(key);
                if (oldValue == null)
                {
                    // no old value exists, which means we can simply rename the instanceName of this prop-entry
                    entriesToOverrideInstanceName.computeIfAbsent(instanceName, ignored -> new HashSet<>())
                        .add(key);
                }
                else
                {
                    // old and new values exist. we obviously have to take the new value
                    entriesToUpdateValues.computeIfAbsent(instanceName, ignored -> new HashMap<>())
                        .put(key, propEntry.getValue());
                }
            }
        }
    }

    private static void fixVlmConProps(Entry<String, Map<String, String>> entryRef, Changes changesRef)
    {
        // Volume connections were implemented but never exposed to the user. There should be no properties on volume
        // connections at all, or even if they do exist they were certainly not created by the user. Therefore it is
        // enough to simply rename the instanceName unconditionally.

        unconditionalRenameInstanceName(entryRef, changesRef);
    }


    private static void fixSnapsRscProps(Entry<String, Map<String, String>> entryRef, Changes retRef)
    {
        // unlike RscGrp properties, SnapsRsc properties could not have been modified since the migration. That means
        // that we can just unconditionally rename the instanceName

        unconditionalRenameInstanceName(entryRef, retRef);
    }

    private static void unconditionalRenameInstanceName(Entry<String, Map<String, String>> entryRef, Changes changesRef)
    {
        changesRef.entriesToOverrideInstanceName.put(entryRef.getKey(), entryRef.getValue().keySet());
    }

    public static String getNewInstanceName(String origInstanceNameRef) throws DatabaseException
    {
        final String search;
        final String replace;
        if (origInstanceNameRef.startsWith(INSTANCE_NAME_RSC_GRP_PREFIX_OLD))
        {
            search = INSTANCE_NAME_RSC_GRP_PREFIX_OLD;
            replace = INSTANCE_NAME_RSC_GRP_PREFIX_NEW;
        }
        else if (origInstanceNameRef.startsWith(INSTANCE_NAME_SNAPS_RSC_PREFIX_OLD))
        {
            search = INSTANCE_NAME_SNAPS_RSC_PREFIX_OLD;
            replace = INSTANCE_NAME_SNAPS_RSC_PREFIX_NEW;
        }
        else if (origInstanceNameRef.startsWith(INSTANCE_NAME_VLM_CON_PREFIX_OLD))
        {
            search = INSTANCE_NAME_VLM_CON_PREFIX_OLD;
            replace = INSTANCE_NAME_VLM_CON_PREFIX_NEW;
        }
        else
        {
            throw new DatabaseException("Unexpected instanceName: '" + origInstanceNameRef + "'");
        }
        return origInstanceNameRef.replaceFirst(search, replace);
    }

    public static class Changes
    {
        public final Map<String, Set<String>> entriesToOverrideInstanceName = new HashMap<>();
        public final Map<String, Map<String, String>> entriesToUpdateValues = new HashMap<>();
    }
}
