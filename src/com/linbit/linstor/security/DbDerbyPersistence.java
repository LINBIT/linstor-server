package com.linbit.linstor.security;

import static com.linbit.linstor.security.SecurityDbFields.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.linbit.linstor.logging.ErrorReporter;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DbDerbyPersistence implements DbAccessor
{
    private static final String SLCT_SIGNIN_ENTRY =
        "SELECT " +
        TBL_IDENTITIES + "." + IDENTITY_NAME + ", " +
        ID_LOCKED + ", " + ID_ENABLED + ", " +
        PASS_SALT + ", " + PASS_HASH + ", " +
        TBL_DFLT_ROLES + "." + ROLE_NAME + ", " +
        TBL_ROLES + "." + DOMAIN_NAME + ", " +
        TBL_ROLES + "." + ROLE_PRIVILEGES + " " +
        "FROM " + TBL_IDENTITIES + "\n" +
        "    LEFT JOIN " + TBL_DFLT_ROLES + " ON " + TBL_IDENTITIES + "." + IDENTITY_NAME + " = " +
        TBL_DFLT_ROLES + "." + IDENTITY_NAME + "\n" +
        "    LEFT JOIN " + TBL_ROLES + " ON " + TBL_DFLT_ROLES + "." + ROLE_NAME + " = " +
        TBL_ROLES + "." + ROLE_NAME + "\n" +
        "    WHERE " + TBL_IDENTITIES + "." + IDENTITY_NAME + " = ?";

    private static final String SLCT_ID_ROLE_MAP_ENTRY =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_ID_ROLE_MAP +
        " WHERE " + IDENTITY_NAME + " = ?" +
        " AND " + ROLE_NAME + " = ?";

    private static final String SLCT_DFLT_ROLE =
        "SELECT " + IDENTITY_NAME + ", " + ROLE_NAME + " FROM " + TBL_DFLT_ROLES +
        " WHERE " + IDENTITY_NAME + " = '?'";

    private static final String SLCT_IDENTITIES =
        "SELECT * FROM " + VW_IDENTITIES_LOAD;

    private static final String SLCT_SEC_TYPES =
        "SELECT * FROM " + VW_TYPES_LOAD;

    private static final String SLCT_ROLES =
        "SELECT * FROM " + VW_ROLES_LOAD;

    private static final String SLCT_TE_RULES =
        "SELECT * FROM " + VW_TYPE_RULES_LOAD;

    private static final String SLCT_SEC_LEVEL =
        "SELECT " + CONF_KEY + ", " + CONF_VALUE +
        " FROM " + TBL_SEC_CFG +
        " WHERE " + CONF_KEY + " = '" + KEY_SEC_LEVEL + "'";

    private static final String DEL_SEC_LEVEL =
        "DELETE FROM " + TBL_SEC_CFG + " WHERE " + CONF_KEY + " = '" + KEY_SEC_LEVEL + "'";

    private static final String INS_SEC_LEVEL =
        "INSERT INTO " + TBL_SEC_CFG + " (" + CONF_KEY + ", " + CONF_DSP_KEY + ", " + CONF_VALUE +
        ") VALUES('" + KEY_SEC_LEVEL + "', '" + KEY_DSP_SEC_LEVEL + "', ?)";

    private final ObjectProtectionDatabaseDriver objProtDriver;

    public DbDerbyPersistence(AccessContext privCtx, ErrorReporter errorReporter)
    {
        objProtDriver = new ObjectProtectionDerbyDriver(privCtx, errorReporter);
    }

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return dbQuery(
            dbConn,
            SLCT_SIGNIN_ENTRY,
            new String[] { idName.value }
        );
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName)
        throws SQLException
    {
        return dbQuery(
            dbConn,
            SLCT_ID_ROLE_MAP_ENTRY,
            new String[] { idName.value, rlName.value }
        );
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return dbQuery(dbConn, SLCT_DFLT_ROLE, new String[] { idName.value });
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

    public void setSecurityLevel(Connection dbConn, SecurityLevel newLevel) throws SQLException
    {
        // Delete any existing security level entry
        {
            Statement delStmt = dbConn.createStatement();
            delStmt.execute(DEL_SEC_LEVEL);
        }

        // Insert the new security level entry
        {
            PreparedStatement insStmt = dbConn.prepareStatement(INS_SEC_LEVEL);
            insStmt.setString(1, newLevel.name().toUpperCase());
            insStmt.execute();
        }

        dbConn.commit();
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver()
    {
        return objProtDriver;
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
