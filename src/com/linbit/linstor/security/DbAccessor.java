package com.linbit.linstor.security;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Database interface for security objects persistence
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DbAccessor
{
    ResultSet getSignInEntry(Connection dbConn, IdentityName idName)
        throws SQLException;
    ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName)
        throws SQLException;
    ResultSet getDefaultRole(Connection dbConn, IdentityName idName)
        throws SQLException;

    ResultSet loadIdentities(Connection dbConn)
        throws SQLException;
    ResultSet loadSecurityTypes(Connection dbConn)
        throws SQLException;
    ResultSet loadRoles(Connection dbConn)
        throws SQLException;
    ResultSet loadTeRules(Connection dbConn)
        throws SQLException;

    ResultSet loadSecurityLevel(Connection dbConn)
        throws SQLException;
    ResultSet loadAuthRequired(Connection dbConn)
        throws SQLException;
    void setSecurityLevel(Connection dbConn, SecurityLevel newLevel)
        throws SQLException;
    void setAuthRequired(Connection dbConn, boolean newPolicy)
        throws SQLException;
}
