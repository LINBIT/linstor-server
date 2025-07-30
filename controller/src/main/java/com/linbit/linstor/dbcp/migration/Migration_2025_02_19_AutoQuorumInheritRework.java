package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.02.19.14.00",
    description = "Auto-quorum rework"
)
public class Migration_2025_02_19_AutoQuorumInheritRework extends LinstorMigration
{
    private static final String DELETE_AUTO_QUORUM_PROP =
        "DELETE FROM PROPS_CONTAINERS WHERE PROP_KEY='DrbdOptions/auto-quorum'";
    private static final String SELECT_AUTO_QUORUM_PROP = "SELECT PROPS_INSTANCE, PROP_KEY, PROP_VALUE " +
        "FROM PROPS_CONTAINERS WHERE PROP_KEY='DrbdOptions/auto-quorum'";
    private static final String SELECT_QUORUM_PROP = "SELECT PROPS_INSTANCE, PROP_VALUE " +
        "FROM PROPS_CONTAINERS WHERE PROPS_INSTANCE=? AND PROP_KEY='DrbdOptions/Resource/quorum'";
    private static final String SELECT_ON_NO_QUORUM_PROP = "SELECT PROPS_INSTANCE, PROP_VALUE " +
        "FROM PROPS_CONTAINERS WHERE PROPS_INSTANCE=? AND PROP_KEY='DrbdOptions/Resource/on-no-quorum'";
    private static final String INSERT_AUTO_QUORUM_TO_SET_BY = "INSERT INTO PROPS_CONTAINERS " +
        "(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) " +
        "VALUES ( ?, 'Internal/Drbd/QuorumSetBy', 'user')";
    private static final String INSERT_QUORUM_OFF = "INSERT INTO PROPS_CONTAINERS " +
        "(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) " +
        "VALUES ( ?, 'DrbdOptions/Resource/quorum', 'off')";
    private static final String INSERT_ON_NO_QUORUM_PROP = "INSERT INTO " +
        "PROPS_CONTAINERS(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) VALUES " +
        "( ?, 'DrbdOptions/Resource/on-no-quorum', ?)";


    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectAutoQuorumProp = conRef.prepareStatement(SELECT_AUTO_QUORUM_PROP);
            PreparedStatement delAutoQuorumStmt = conRef.prepareStatement(DELETE_AUTO_QUORUM_PROP);
            PreparedStatement insQuorumSetByStmt = conRef.prepareStatement(INSERT_AUTO_QUORUM_TO_SET_BY);
            PreparedStatement selQuorumPropStmt = conRef.prepareStatement(SELECT_QUORUM_PROP);
            PreparedStatement selOnNoQuorumPropStmt = conRef.prepareStatement(SELECT_ON_NO_QUORUM_PROP);
            PreparedStatement insQuorumOffStmt = conRef.prepareStatement(INSERT_QUORUM_OFF);
            PreparedStatement insOnNoQuorumStmt = conRef.prepareStatement(INSERT_ON_NO_QUORUM_PROP);
        )
        {
            // Loop all auto-quorum properties
            try (ResultSet rsAutoQuorumProp = selectAutoQuorumProp.executeQuery())
            {
                while (rsAutoQuorumProp.next())
                {
                    String autoQuorumValue = rsAutoQuorumProp.getString("PROP_VALUE");
                    String propInstance = rsAutoQuorumProp.getString("PROPS_INSTANCE");
                    propInstance = propInstance.equalsIgnoreCase("/CTRL") ? "/STLT" : propInstance;

                    if (autoQuorumValue.equalsIgnoreCase("disabled"))
                    {
                        // if disabled set quorum -> off for the props_instance if not already set
                        insQuorumSetByStmt.setString(1, propInstance);
                        insQuorumSetByStmt.executeUpdate();

                        selQuorumPropStmt.setString(1, propInstance);
                        try (ResultSet rsQuorumProp = selQuorumPropStmt.executeQuery())
                        {
                            if (!rsQuorumProp.next())
                            {
                                insQuorumOffStmt.setString(1, propInstance);
                                insQuorumOffStmt.executeUpdate();
                            }
                        }
                    }
                    else
                    {
                        // if not disabled, set the current auto-quorum value to on-no-quorum
                        selOnNoQuorumPropStmt.setString(1, propInstance);
                        try (ResultSet rsOnNoQuorumProp = selOnNoQuorumPropStmt.executeQuery())
                        {
                            if (!rsOnNoQuorumProp.next())
                            {
                                insOnNoQuorumStmt.setString(1, propInstance);
                                insOnNoQuorumStmt.setString(2, autoQuorumValue);
                                insOnNoQuorumStmt.executeUpdate();
                            }
                        }
                    }
                }
            }

            delAutoQuorumStmt.executeUpdate();
        }
    }
}
