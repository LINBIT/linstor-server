package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linbit.ObjectDatabaseDriver;

public class EmptySecurityDbDriver implements DbAccessor
{

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadIdentities(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadSecurityTypes(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadRoles(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadTeRules(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver(String objectPath)
    {
        return new EmptyObjectProtectionDatabaseDriver(objectPath);
    }

    private static class EmptyObjectProtectionDatabaseDriver implements ObjectProtectionDatabaseDriver
    {
        public EmptyObjectProtectionDatabaseDriver(String objectPath)
        {
        }

        @Override
        public void insertOp(Connection con, ObjectProtection objProt) throws SQLException
        {
            // no-op
        }

        @Override
        public void updateOp(Connection con, ObjectProtection objProt) throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteOp(Connection con) throws SQLException
        {
            // no-op
        }

        @Override
        public void insertAcl(Connection con, Role role, AccessType grantedAccess) throws SQLException
        {
            // no-op
        }

        @Override
        public void updateAcl(Connection con, Role role, AccessType grantedAccess) throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteAcl(Connection con, Role role) throws SQLException
        {
            // no-op
        }

        @Override
        public ObjectProtection loadObjectProtection(Connection con) throws SQLException
        {
            // no-op
            return null;
        }

        @Override
        public ObjectDatabaseDriver<Identity> getIdentityDatabaseDrier()
        {
            // no-op
            return null;
        }

        @Override
        public ObjectDatabaseDriver<Role> getRoleDatabaseDriver()
        {
            // no-op
            return null;
        }

        @Override
        public ObjectDatabaseDriver<SecurityType> getSecurityTypeDriver()
        {
            // no-op
            return null;
        }

    }
}
