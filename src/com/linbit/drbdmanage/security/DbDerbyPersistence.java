package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_IDENTITIES;
import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_ROLES;
import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_ID_ROLE_MAP;
import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_DFLT_ROLES;
import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_OBJ_PROT;
import static com.linbit.drbdmanage.security.SecurityDbFields.TBL_ACL_MAP;

import static com.linbit.drbdmanage.security.SecurityDbFields.VW_IDENTITIES_LOAD;
import static com.linbit.drbdmanage.security.SecurityDbFields.VW_ROLES_LOAD;
import static com.linbit.drbdmanage.security.SecurityDbFields.VW_TYPES_LOAD;
import static com.linbit.drbdmanage.security.SecurityDbFields.VW_TYPE_RULES_LOAD;

import static com.linbit.drbdmanage.security.SecurityDbFields.IDENTITY_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.CRT_IDENTITY_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.OWNER_ROLE_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.SEC_TYPE_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.OBJECT_PATH;
import static com.linbit.drbdmanage.security.SecurityDbFields.ID_ENABLED;
import static com.linbit.drbdmanage.security.SecurityDbFields.ID_LOCKED;
import static com.linbit.drbdmanage.security.SecurityDbFields.PASS_SALT;
import static com.linbit.drbdmanage.security.SecurityDbFields.PASS_HASH;
import static com.linbit.drbdmanage.security.SecurityDbFields.ROLE_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.DOMAIN_NAME;
import static com.linbit.drbdmanage.security.SecurityDbFields.ACCESS_TYPE;

import static com.linbit.drbdmanage.security.SecurityDbFields.CONF_KEY;
import static com.linbit.drbdmanage.security.SecurityDbFields.CONF_VALUE;

import static com.linbit.drbdmanage.security.SecurityDbFields.KEY_SEC_LEVEL;
import java.sql.Statement;

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
        TBL_ROLES + "." + DOMAIN_NAME + " " +
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

    private static final String SLCT_OBJ_PROT =
        "SELECT " + OBJECT_PATH + ", " + CRT_IDENTITY_NAME + ", " + OWNER_ROLE_NAME +
        ", " + SEC_TYPE_NAME +
        " FROM " + TBL_OBJ_PROT +
        " WHERE OBJECT_PATH = ?";

    private static final String SLCT_ACL_ENTRIES =
        "SELECT " + OBJECT_PATH + ", " + ROLE_NAME + ", " + ACCESS_TYPE +
        " FROM " + TBL_ACL_MAP +
        " WHERE " + OBJECT_PATH + " = ?";


    private static final String SLCT_SEC_LEVEL =
        "SELECT " + CONF_KEY + ", " + CONF_VALUE +
        " FROM " + TBL_ACL_MAP +
        " WHERE " + CONF_KEY + " = " + KEY_SEC_LEVEL;

    private AccessContext dbCtx;

    public DbDerbyPersistence(AccessContext privCtx)
    {
        dbCtx = privCtx;
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

//    @Override
//    public ResultSet loadObjectProtection(Connection dbConn, String objectPath) throws SQLException
//    {
//        return dbQuery(dbConn, SLCT_OBJ_PROT, new String[] { objectPath });
//    }
//
//    @Override
//    public ResultSet loadAclEntries(Connection dbConn, String objectPath) throws SQLException
//    {
//        return dbQuery(dbConn, SLCT_ACL_ENTRIES, new String[] { objectPath });
//    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return dbQuery(dbConn, SLCT_SEC_LEVEL);
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver(String objectPath)
    {
        return new ObjectProtectionDerbyDriver(dbCtx, objectPath);
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
            stmt.setString(idx, arguments[idx]);
        }
        return stmt.executeQuery();
    }
}
