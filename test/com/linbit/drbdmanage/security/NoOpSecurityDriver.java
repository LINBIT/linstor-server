package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NoOpSecurityDriver implements DbAccessor
{

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadIdentities(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadSecurityTypes(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadRoles(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadTeRules(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver()
    {
        return null; // intentionally null
    }
}
