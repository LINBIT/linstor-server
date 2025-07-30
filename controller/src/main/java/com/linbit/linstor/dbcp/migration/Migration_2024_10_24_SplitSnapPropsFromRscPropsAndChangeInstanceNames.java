package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps;
import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps.InstanceType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2024.10.24.10.00",
    description = "Split snapshotted resource properties from snapshot properties and change instance names"
)
public class Migration_2024_10_24_SplitSnapPropsFromRscPropsAndChangeInstanceNames extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";

    private static final String SELECT_STMT = "SELECT " + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS;

    private static final String UPDATE_STMT = "UPDATE " + TBL_PROPS_CONTAINERS + " SET " + PROPS_INSTANCE + " = ? " +
        "WHERE " + PROPS_INSTANCE + " = ? AND " + PROP_KEY + " = ? ";
    private static final int UPDATE_STMT_SET_PROPS_INSTANCE_ID = 1;
    private static final int UPDATE_STMT_QUERY_PROPS_INSTANCE_ID = 2;
    private static final int UPDATE_STMT_QUERY_PROPS_KEY_ID = 3;

    private static final int SEPARATOR_COUNT_FOR_SNAPSHOTS = 4;

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
        final InstanceType instanceType = getSnapObjByInstanceName(origInstanceNameRef);

        final String newPrefix = MigrationUtils_SplitSnapProps.useAlternativePrefix(instanceType, keyRef) ?
            instanceType.getNewAlternativePrefix() :
            instanceType.getNewDfltPrefix();

        return newPrefix + origInstanceNameRef.substring(instanceType.getOrigPrefix().length());
    }

    private static InstanceType getSnapObjByInstanceName(String origInstanceNameRef)
    {
        @Nullable InstanceType ret = null;
        for (InstanceType type : InstanceType.values())
        {
            if (origInstanceNameRef.startsWith(type.getOrigPrefix()))
            {
                if (type == InstanceType.SNAP || type == InstanceType.SNAP_VLM)
                {
                    /*
                     * these two instance types had the same original prefix "/SNAPSHOTS/". The difference was that
                     * actual snapshots only had 3 parts for their key: nodeName, rscName and snapName, whereas snapVlms
                     * had an additional vlmNr.
                     *
                     * example instance names (note the missing trailing "/"):
                     * * snapshot: "/SNAPSHOTS/NODE/RSC/SNAP"
                     * * snapVlm : "/SNAPSHOTS/NODE/RSC/SNAP/0"
                     */
                    ret = origInstanceNameRef.replaceAll("[^/]", "").length() == SEPARATOR_COUNT_FOR_SNAPSHOTS ?
                        InstanceType.SNAP :
                        InstanceType.SNAP_VLM;
                }
                else
                {
                    ret = type;
                }
                break;
            }
        }
        if (ret == null)
        {
            // current version of linstor is not using / setting special property on snapshotVolume
            throw new ImplementationError("Unrecognized instance name type: \"" + origInstanceNameRef + "\"");
        }
        return ret;
    }
}
