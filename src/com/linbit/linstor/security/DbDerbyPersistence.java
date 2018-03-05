package com.linbit.linstor.security;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ENTRY_DSP_KEY;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ENTRY_KEY;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ENTRY_VALUE;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ID_ENABLED;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ID_LOCKED;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.PASS_HASH;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.PASS_SALT;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ROLE_NAME;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.ROLE_PRIVILEGES;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_CONFIGURATION;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_DFLT_ROLES;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_IDENTITIES;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_ID_ROLE_MAP;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_ROLES;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.VIEW_SEC_IDENTITIES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.VIEW_SEC_ROLES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.VIEW_SEC_TYPES_LOAD;
import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.VIEW_SEC_TYPE_RULES_LOAD;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DbDerbyPersistence implements DbAccessor
{
    private static final String SLCT_SIGNIN_ENTRY =
        "SELECT " +
        TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + ", " +
        ID_LOCKED + ", " + ID_ENABLED + ", " +
        PASS_SALT + ", " + PASS_HASH + ", " +
        TBL_SEC_DFLT_ROLES + "." + ROLE_NAME + ", " +
        TBL_SEC_ROLES + "." + DOMAIN_NAME + ", " +
        TBL_SEC_ROLES + "." + ROLE_PRIVILEGES + " " +
        "FROM " + TBL_SEC_IDENTITIES + "\n" +
        "    LEFT JOIN " + TBL_SEC_DFLT_ROLES + " ON " + TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + " = " +
        TBL_SEC_DFLT_ROLES + "." + IDENTITY_NAME + "\n" +
        "    LEFT JOIN " + TBL_SEC_ROLES + " ON " + TBL_SEC_DFLT_ROLES + "." + ROLE_NAME + " = " +
        TBL_SEC_ROLES + "." + ROLE_NAME + "\n" +
        "    WHERE " + TBL_SEC_IDENTITIES + "." + IDENTITY_NAME + " = ?";

    private static final String SLCT_ID_ROLE_MAP_ENTRY =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_SEC_ID_ROLE_MAP +
        " WHERE " + IDENTITY_NAME + " = ?" +
        " AND " + ROLE_NAME + " = ?";

    private static final String SLCT_DFLT_ROLE =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_SEC_DFLT_ROLES +
        " WHERE " + IDENTITY_NAME + " = '?'";

    private static final String SLCT_IDENTITIES =
        "SELECT * FROM " + VIEW_SEC_IDENTITIES_LOAD;

    private static final String SLCT_SEC_TYPES =
        "SELECT * FROM " + VIEW_SEC_TYPES_LOAD;

    private static final String SLCT_ROLES =
        "SELECT * FROM " + VIEW_SEC_ROLES_LOAD;

    private static final String SLCT_TE_RULES =
        "SELECT * FROM " + VIEW_SEC_TYPE_RULES_LOAD;

    private static final String SLCT_SEC_LEVEL =
        "SELECT " + ENTRY_KEY + ", " + ENTRY_VALUE +
        " FROM " + TBL_SEC_CONFIGURATION +
        " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_SEC_LEVEL + "'";

    private static final String SLCT_AUTH_REQ =
        "SELECT " + ENTRY_KEY + ", " + ENTRY_VALUE +
        " FROM " + TBL_SEC_CONFIGURATION +
        " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_AUTH_REQ + "'";

    private static final String DEL_SEC_LEVEL =
        "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_SEC_LEVEL + "'";

    private static final String INS_SEC_LEVEL =
        "INSERT INTO " + TBL_SEC_CONFIGURATION + " (" + ENTRY_KEY + ", " + ENTRY_DSP_KEY + ", " + ENTRY_VALUE +
        ") VALUES('" + SecurityDbConsts.KEY_SEC_LEVEL + "', '" + SecurityDbConsts.KEY_DSP_SEC_LEVEL + "', ?)";

    private static final String DEL_AUTH_REQUIRED =
        "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE " + ENTRY_KEY + " = '" + SecurityDbConsts.KEY_AUTH_REQ + "'";

    private static final String INS_AUTH_REQUIRED =
        "INSERT INTO " + TBL_SEC_CONFIGURATION + " (" + ENTRY_KEY + ", " + ENTRY_DSP_KEY + ", " + ENTRY_VALUE +
        ") VALUES('" + SecurityDbConsts.KEY_AUTH_REQ + "', '" + SecurityDbConsts.KEY_DSP_AUTH_REQ + "', ?)";

    @Inject
    public DbDerbyPersistence()
    {
    }

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return dbQuery(
            dbConn,
            SLCT_SIGNIN_ENTRY,
            new String[] {idName.value}
        );
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName)
        throws SQLException
    {
        return dbQuery(
            dbConn,
            SLCT_ID_ROLE_MAP_ENTRY,
            new String[] {idName.value, rlName.value}
        );
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return dbQuery(dbConn, SLCT_DFLT_ROLE, new String[] {idName.value});
    }

    @Override
    public ResultSet loadIdentities(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_IDENTITIES);
    }

    @Override
    public ResultSet loadSecurityTypes(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_SEC_TYPES);
    }

    @Override
    public ResultSet loadRoles(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_ROLES);
    }

    @Override
    public ResultSet loadTeRules(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_TE_RULES);
    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_SEC_LEVEL);
    }

    @Override
    public ResultSet loadAuthRequired(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_AUTH_REQ);
    }

    @Override
    public void setSecurityLevel(Connection dbConn, SecurityLevel newLevel) throws SQLException
    {
        try
        {
            // Delete any existing security level entry
            try (Statement delStmt = dbConn.createStatement())
            {

                delStmt.execute(DEL_SEC_LEVEL);
            }

            // Insert the new security level entry
            try (PreparedStatement insStmt = dbConn.prepareStatement(INS_SEC_LEVEL))
            {
                insStmt.setString(1, newLevel.name().toUpperCase());
                insStmt.execute();
            }

            dbConn.commit();
        }
        catch (SQLException sqlExc)
        {
            dbConn.rollback();
        }
    }

    @Override
    public void setAuthRequired(Connection dbConn, boolean requiredFlag) throws SQLException
    {
        try
        {
            // Delete any existing authentication requirement entry
            try (Statement delStmt = dbConn.createStatement())
            {
                delStmt.execute(DEL_AUTH_REQUIRED);
            }

            // Insert the new authentication requirement
            try (PreparedStatement insStmt = dbConn.prepareStatement(INS_AUTH_REQUIRED))
            {

                String dbValue = requiredFlag ? Boolean.toString(true) : Boolean.toString(false);
                insStmt.setString(1, dbValue);
            }

            dbConn.commit();
        }
        catch (SQLException sqlExc)
        {
            dbConn.rollback();
            throw sqlExc;
        }
    }


    private ResultSet dbQuery(Connection dbConn, String sqlQuery) throws SQLException
    {
        Statement stmt = dbConn.createStatement();
        return stmt.executeQuery(sqlQuery);
    }

    private ResultSet dbQuery(Connection dbConn, String sqlQuery, String[] arguments) throws SQLException
    {
        PreparedStatement stmt = dbConn.prepareStatement(sqlQuery);
        for (int idx = 0; idx < arguments.length; ++idx)
        {
            stmt.setString(idx + 1, arguments[idx]);
        }
        return stmt.executeQuery();
    }
}
